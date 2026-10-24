"""consumidor_kafka.py — Lab 2a (ST1630-2026-2, S6-S7)

Lee pedidos del topic "pedidos-ventas" y los ingesta en Bronze del
datalake (mismo patrón MERGE Delta del Lab 1b) con garantía
at-least-once real: el offset solo se commitea DESPUÉS de que el
MERGE terminó con éxito.

Entrega del equipo: los bloques 2.1, 2.2 y 2.3 que el script traía
marcados como TODO están implementados abajo. El MERGE Delta venía
dado (es el del Lab 1b, adaptado a un mensaje de Kafka en vez de un
batch de CSV); lo nuevo de esta semana -- y el núcleo del lab -- es la
coreografía de cuándo commitear el offset, en el for de main().

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

# ── Tuning propio, no venía en el enunciado ───────────────────────────
# Las dos primeras .config() son las del enunciado. Las tres siguientes las
# agregué porque, sin ellas, el consumidor no alcanza a terminar los 1.000
# mensajes: el tiempo por MERGE arrancó en ~5 s y a los 120 mensajes ya iba
# en ~20 s, subiendo. La causa es que cada mensaje produce su propio archivo
# Parquet de una fila, y el MERGE tiene que escanear TODOS los archivos
# existentes para buscar el match por pedido_id -- así que el costo por
# mensaje crece con el número de mensajes ya ingestados (comportamiento
# cuadrático en el total). Medición: 124 mensajes -> 124 archivos, 3,0 MB.
#
#   shuffle.partitions=4  : el default (200) crea 200 tareas de shuffle para
#                           hacer el join de UNA fila contra la tabla. Con 4
#                           particiones el overhead de scheduling deja de
#                           dominar el tiempo de cada MERGE.
#   optimizeWrite         : agrupa la escritura para no dejar un archivo
#                           diminuto por operación.
#   autoCompact           : compacta automáticamente los archivos pequeños
#                           tras la escritura, que es lo que evita que el
#                           scan del MERGE siga creciendo sin límite.
#
# Esto es tuning, no un cambio de la lógica: el MERGE, su condición de match
# y la coreografía del commit siguen siendo exactamente los mismos. La
# solución de fondo sigue siendo procesar por lotes -- ver kafka_design.md,
# Pregunta 5(c).
spark = (
    SparkSession.builder.appName("ST1630-Lab2a-Consumidor")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ═══════════════════════════════════════════════════════════════
# 2.1 · Configuración del KafkaConsumer
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
# Configuración aplicada:
#   - TOPIC como primer argumento posicional
#   - bootstrap_servers=[KAFKA_BOOTSTRAP]
#   - group_id=GROUP_ID
#   - auto_offset_reset="earliest"
#   - enable_auto_commit=False
#   - value_deserializer: función que reciba bytes y devuelva un dict
#     (json.loads(v.decode("utf-8")))
#   - key_deserializer: función que reciba bytes (o None) y devuelva
#     un string (o None)
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    # ── La línea que define la garantía de todo el pipeline ──
    # Con False, Kafka NO avanza el offset por su cuenta: el único que
    # decide qué se considera "leído" es el consumer.commit() de abajo,
    # que se ejecuta después del MERGE. Ese orden es lo que convierte
    # este consumidor en at-least-once en vez de at-most-once.
    enable_auto_commit=False,
    # ── max_poll_records=1: ajuste propio, no venía en el enunciado ──
    # Con el valor por defecto (500) el consumidor se traía 500 mensajes de
    # una y recién volvía a llamar a poll() cuando terminaba de procesarlos
    # todos. Como cada mensaje cuesta un MERGE Delta de ~5 s, entre dos
    # poll() pasaban ~40 min: muchísimo más que max_poll_interval_ms (300 s
    # por defecto). El broker daba al consumidor por muerto, lo expulsaba
    # del grupo y todos los commit posteriores fallaban con:
    #
    #   CommitFailedError: Offset commit cannot be completed since the
    #   consumer is not part of an active group [...] it is likely that the
    #   consumer was kicked out of the group.
    #
    # Con 1 registro por poll, entre llamadas pasan ~5 s y la sesión nunca
    # expira. La evidencia del fallo está en
    # ../datos/consumidor_expulsion_evidencia.txt y el análisis en
    # ../kafka_design.md (Pregunta 5c): la solución de fondo para volumen
    # alto no es bajar max_poll_records sino procesar por lotes, para que
    # el costo del MERGE se amortice entre miles de filas.
    max_poll_records=1,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    # La key puede llegar como None (mensajes producidos sin key), así que
    # el deserializer tiene que tolerarlo igual que el del productor.
    key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
)


def construir_fila_bronze(mensaje) -> dict:
    """A partir de un ConsumerRecord de kafka-python, arma el dict que
    se va a escribir en Bronze -- el pedido tal cual llegó, más 4
    columnas de trazabilidad. Estas columnas son un patrón de
    producción real: te permiten reconstruir, para cualquier fila de
    Bronze, exactamente de qué topic/partición/offset de Kafka vino --
    útil para debugging y para auditorías de linaje de datos."""
    pedido = dict(mensaje.value)
    pedido["_kafka_offset"] = mensaje.offset
    pedido["_kafka_partition"] = mensaje.partition
    pedido["_kafka_topic"] = mensaje.topic
    # _ingested_at es hora de INGESTA, no del evento: si el mismo mensaje se
    # reprocesa tras una caída, el MERGE lo pisa con un _ingested_at nuevo.
    # Por eso la fila no se duplica pero sí queda constancia de que se
    # volvió a escribir -- útil para auditar un reprocesamiento.
    pedido["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    return pedido


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
# 2.2 / 2.3 · Loop principal -- procesar y commitear
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
# Implementado en el for de main() con este try/except:
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
        try:
            fila = construir_fila_bronze(mensaje)
            merge_a_bronze(fila)
            # ── El commit va AQUÍ y en ningún otro lado ──
            # Si el proceso muere una línea antes de esta, el MERGE ya pasó
            # pero Kafka nunca se enteró: al reiniciar vuelve a entregar
            # este mismo offset y el MERGE lo escribe otra vez sobre la
            # misma fila (mismo pedido_id) sin duplicarla. Ese es el
            # escenario exacto de la prueba de idempotencia.
            consumer.commit()
            contador_procesados += 1
            print(f"[OK] offset={mensaje.offset} partition={mensaje.partition} "
                  f"pedido_id={fila['pedido_id']}", flush=True)
        except Exception as e:
            # Sin commit a propósito: dejar el offset donde está es lo que
            # hace que Kafka reentregue el mensaje en el próximo arranque.
            contador_rechazados += 1
            print(f"[ERROR] offset={mensaje.offset} partition={mensaje.partition} "
                  f"no se commiteó -- se reprocesará. Causa: {e}", flush=True)

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
