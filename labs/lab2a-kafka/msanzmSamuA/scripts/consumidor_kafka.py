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
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
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
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
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
# TODO 2.2 / 2.3 · Loop principal -- procesar y commitear
# ═══════════════════════════════════════════════════════════════
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
            consumer.commit()
            contador_procesados += 1
            print(f"[OK] offset={mensaje.offset} partition={mensaje.partition} "
                  f"pedido_id={fila['pedido_id']}")
        except Exception as e:
            contador_rechazados += 1
            print(f"[ERROR] offset={mensaje.offset} partition={mensaje.partition} "
                  f"no se commiteó -- se reprocesará. Causa: {e}")

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
