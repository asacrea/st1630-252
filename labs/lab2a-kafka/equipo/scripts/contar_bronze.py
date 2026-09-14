"""contar_bronze.py — utilidad de verificación del Lab 2a (equipo)

Cuenta las filas de la tabla Delta de Bronze y reporta cuántos pedido_id
DISTINTOS hay. Esos dos números son la evidencia de la prueba de
idempotencia (Parte 2.4): si el MERGE es idempotente, total == distintos
siempre, aunque Kafka haya reentregado mensajes.

Uso:
    BRONZE_PATH=/lake/bronze/pedidos python3 contar_bronze.py
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = os.environ.get("BRONZE_PATH", "/tmp/lake/bronze/pedidos")
ETIQUETA = sys.argv[1] if len(sys.argv) > 1 else "(sin etiqueta)"

spark = (
    SparkSession.builder.appName("ST1630-Lab2a-ContarBronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(BRONZE_PATH)
total = df.count()
distintos = df.select("pedido_id").distinct().count()

print(f"=== CONTEO BRONZE [{ETIQUETA}] ===")
print(f"filas_totales      = {total}")
print(f"pedido_id_distintos= {distintos}")
print(f"duplicados         = {total - distintos}")

print("\n--- filas por partición de Kafka de origen ---")
(df.groupBy("_kafka_partition").count().orderBy("_kafka_partition")).show()

print("--- rango de offsets ingestados por partición ---")
(
    df.groupBy("_kafka_partition")
    .agg(F.min("_kafka_offset").alias("offset_min"), F.max("_kafka_offset").alias("offset_max"))
    .orderBy("_kafka_partition")
).show()

print("--- ultimas filas ingestadas (para rastrear un reproceso) ---")
# _ingested_at es la marca de tiempo de la INGESTA. Si un mensaje se
# reprocesa tras una caida, el MERGE pisa la fila y este valor cambia,
# pero la fila sigue siendo UNA sola: esa es la prueba visible de que el
# MERGE es idempotente y no un append.
(
    df.select("_kafka_partition", "_kafka_offset", "pedido_id", "_ingested_at")
    .orderBy(F.col("_kafka_partition").asc(), F.col("_kafka_offset").desc())
    .limit(5)
).show(truncate=False)

spark.stop()
