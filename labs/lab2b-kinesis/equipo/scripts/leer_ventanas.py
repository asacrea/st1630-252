"""leer_ventanas.py — utilidad de verificación del Lab 2b (equipo)

Lee la tabla Delta que escribe streaming_pipeline.py y reporta las tres
cosas que prueban que el sink foreachBatch + MERGE es idempotente:

  1. Las filas por ventana+región, con sus agregados actuales.
  2. Cuántas combinaciones (window_start, window_end, region) aparecen
     más de una vez. Si el MERGE está bien, siempre es 0; con append,
     cada actualización de una ventana abierta dejaría una fila extra.
  3. El historial de Delta: un WRITE inicial y después solo MERGE.

Uso (desde labs/lab2b-kinesis/, con el volumen del pipeline):
    docker run --rm -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake \
      -v lab2a-ivy:/root/.ivy2 st1630-lab2a-spark \
      python /scripts/leer_ventanas.py "despues de la corrida 3"
"""

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_STREAMING_PATH = os.environ.get("SILVER_STREAMING_PATH", "/tmp/lake/silver/ventas_streaming")
ETIQUETA = sys.argv[1] if len(sys.argv) > 1 else "(sin etiqueta)"

_builder = (
    SparkSession.builder.appName("ST1630-Lab2b-LeerVentanas")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(_builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(SILVER_STREAMING_PATH)
filas = df.count()
pedidos = df.agg(F.sum("num_pedidos")).collect()[0][0] or 0
duplicados = (
    df.groupBy("window_start", "window_end", "region")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"=== VENTANAS SILVER [{ETIQUETA}] ===")
print(f"filas_totales             = {filas}")
print(f"suma_num_pedidos          = {pedidos}")
print(f"duplicados_ventana_region = {duplicados}")

print("\n--- agregados por ventana y región (window_* en UTC) ---")
df.orderBy("window_start", "region").show(100, truncate=False)

print("--- historial de la tabla Delta ---")
(
    spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_STREAMING_PATH}`")
    .select("version", "timestamp", "operation")
    .orderBy("version")
).show(truncate=False)

spark.stop()
