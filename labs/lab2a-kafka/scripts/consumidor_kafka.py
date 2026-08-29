"""consumidor_kafka.py — Lab 2a (ST1630-2026-2, S6-S7)

Lee pedidos del topic "pedidos-ventas" y los ingesta en Bronze del
datalake (mismo patrón MERGE Delta del Lab 1b) con garantía
at-least-once real: el offset solo se commitea DESPUÉS de que el
MERGE terminó con éxito.

Este script tiene bloques marcados con # TODO -- son la parte central
del lab. El MERGE Delta en sí ya lo resolviste en el Lab 1b (aquí
viene dado, solo adaptado a un mensaje de Kafka en vez de un batch de
CSV); lo que es nuevo esta semana -- y por eso es tu TODO -- es la
coreografía de cuándo commitear el offset.

Uso:
    python3 consumidor_kafka.py

Qué puedes delegar: boilerplate de kafka-python/PySpark si te trabas
en la sintaxis. Qué NO puedes delegar: enable_auto_commit=False y el
commit manual DESPUÉS del MERGE -- es el objetivo 3 de esta sesión, y
la prueba de idempotencia (Parte 2.4 del README) solo tiene sentido si
tú mismo escribiste esta coreografía.
"""

import json
import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from kafka import KafkaConsumer
from pyspark.sql import Row, SparkSession

# ─────────────────────────────────────────────────────────────
# Configuración -- funciona en local sin cambios; las variables de
# entorno permiten apuntar a otro clúster/datalake sin tocar código.
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
BRONZE_PATH = os.environ.get("BRONZE_PATH", "/tmp/lake/bronze/pedidos")
TOPIC = "pedidos-ventas"
GROUP_ID = "analytics-group"

spark = (
    SparkSession.builder.appName("ST1630-Lab2a-Consumidor")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ═══════════════════════════════════════════════════════════════
# TODO 2.1 · Configuración del KafkaConsumer
# ═══════════════════════════════════════════════════════════════
# group_id="analytics-group": le da nombre a este consumer group. Sin
# group_id, Kafka no puede rastrear offsets consistentemente para tu
# aplicación -- y un nombre distinto te permitiría tener OTRO grupo
# leyendo el mismo topic de forma completamente independiente (p. ej.
# un grupo "fraude-group" leyendo los mismos mensajes para otro fin).
#
# auto_offset_reset="earliest": el productor YA envió sus 1.000
# mensajes antes de que existiera este consumer group -- si usaras
# "latest", tu consumer solo vería mensajes NUEVOS a partir de ahora y
# no leería nada de lo que el productor ya publicó.
#
# enable_auto_commit=False -- LA DECISIÓN MÁS IMPORTANTE de este
# script. Si la dejaras en True (el default), Kafka commitearía el
# offset automáticamente cada 5 segundos SIN IMPORTAR si ya
# terminaste de procesar ese mensaje. Si tu consumidor se cae justo
# entre ese auto-commit y el MERGE a Bronze, Kafka ya "olvidó" ese
# mensaje -- al reiniciar, retomarías DESPUÉS de él, y ese pedido se
# pierde para siempre. Eso es at-most-once silencioso: nunca te
# enteras de que perdiste datos. Con enable_auto_commit=False, TÚ
# controlas exactamente cuándo Kafka considera "leído" un mensaje --
# y en este script, eso pasa solo después de que el MERGE fue exitoso.
#
# TODO: crea el KafkaConsumer con:
#   - TOPIC como primer argumento posicional
#   - bootstrap_servers=[KAFKA_BOOTSTRAP]
#   - group_id=GROUP_ID
#   - auto_offset_reset="earliest"
#   - enable_auto_commit=False
#   - value_deserializer: función que reciba bytes y devuelva un dict
#     (json.loads(v.decode("utf-8")))
#   - key_deserializer: función que reciba bytes (o None) y devuelva
#     un string (o None)
consumer = None  # TODO: reemplaza por tu KafkaConsumer(...)


def construir_fila_bronze(mensaje) -> dict:
    """A partir de un ConsumerRecord de kafka-python, arma el dict que
    se va a escribir en Bronze -- el pedido tal cual llegó, más 4
    columnas de trazabilidad. Estas columnas son un patrón de
    producción real: te permiten reconstruir, para cualquier fila de
    Bronze, exactamente de qué topic/partición/offset de Kafka vino --
    útil para debugging y para auditorías de linaje de datos."""
    pedido = dict(mensaje.value)
    # TODO: agrega estas 4 columnas al dict `pedido` antes de retornarlo:
    #   - "_kafka_offset": mensaje.offset
    #   - "_kafka_partition": mensaje.partition
    #   - "_kafka_topic": mensaje.topic
    #   - "_ingested_at": timestamp actual en ISO 8601
    #     (datetime.now(timezone.utc).isoformat())
    return pedido  # TODO: reemplaza por pedido + las 4 columnas


def merge_a_bronze(fila: dict):
    """MERGE Delta sobre Bronze por pedido_id (dado -- mismo patrón
    del Lab 1b, script 02_silver.py, Parte 3.7).

    Este MERGE es IDEMPOTENTE: si Kafka reenvía el mismo mensaje
    (porque el consumidor falló después del MERGE pero antes del
    commit), la segunda ejecución no duplica el dato en Bronze -- la
    condición de match es pedido_id, único por pedido. Esto es
    exactamente lo que permite usar at-least-once: Kafka puede
    duplicar la entrega, pero Bronze nunca duplica el dato.

    WIDE ❌: el MERGE internamente hace un hash join entre la fila
    nueva y lo que ya existe en Bronze -- genera un Exchange en Spark
    UI (mismo concepto de S5 que viste en el Lab 1b)."""
    df_nuevo = spark.createDataFrame([Row(**fila)])

    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        bronze = DeltaTable.forPath(spark, BRONZE_PATH)
        (
            bronze.alias("existente")
            .merge(df_nuevo.alias("nuevo"), "existente.pedido_id = nuevo.pedido_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df_nuevo.write.format("delta").mode("overwrite").save(BRONZE_PATH)


# ═══════════════════════════════════════════════════════════════
# TODO 2.2 / 2.3 · Loop principal -- procesar y commitear
# ═══════════════════════════════════════════════════════════════
# Esta es la coreografía completa de at-least-once real:
#
#   1. Leer el mensaje (el for ya te lo da)
#   2. construir_fila_bronze(mensaje)          [dado arriba]
#   3. merge_a_bronze(fila)                     [dado arriba]
#   4. SOLO SI el paso 3 no lanzó excepción: consumer.commit()
#   5. Si el paso 3 falla: NO commitear, loggear el offset que falló
#      con su excepción, y seguir (el mensaje se va a reprocesar la
#      próxima vez que el consumer arranque, exactamente como se
#      espera de at-least-once)
#
# TODO: completa el cuerpo del for con un try/except:
#   try:
#       fila = construir_fila_bronze(mensaje)
#       merge_a_bronze(fila)
#       consumer.commit()  # <- SOLO aquí, después del MERGE exitoso
#       contador_procesados += 1
#       print(f"[OK] offset={mensaje.offset} partition={mensaje.partition} "
#             f"pedido_id={fila['pedido_id']}")
#   except Exception as e:
#       contador_rechazados += 1
#       print(f"[ERROR] offset={mensaje.offset} partition={mensaje.partition} "
#             f"no se commiteó -- se reprocesará. Causa: {e}")
def main():
    contador_procesados = 0
    contador_rechazados = 0

    print(f"Escuchando '{TOPIC}' como grupo '{GROUP_ID}' (bootstrap: {KAFKA_BOOTSTRAP})...")
    print(f"Escribiendo a Bronze en: {BRONZE_PATH}")
    print("Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).\n")

    for mensaje in consumer:
        # TODO: tu try/except aquí (ver especificación arriba)
        raise NotImplementedError("TODO 2.2/2.3: implementa el try/except de procesamiento + commit")

    print(f"\nProcesados: {contador_procesados}  Rechazados (sin commit): {contador_rechazados}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDetenido por el usuario (Ctrl+C). Si fue antes de un commit, "
              "ese mensaje se va a reprocesar en el próximo arranque -- "
              "exactamente el escenario de la prueba de idempotencia.")
    finally:
        consumer.close()
        spark.stop()
