"""ver_silver.py — utilidad de verificación de la Parte 3 (Lab 2b).

Muestra la tabla Silver de ventas por ventana y cuenta cuántas filas hay
por llave (window_start, window_end, region). Si el MERGE del sink es
idempotente, nunca debe haber más de una fila por llave, sin importar
cuántas veces corra el productor.

Uso (dentro del contenedor de Spark, en esta misma carpeta):
    python3 ver_silver.py
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_STREAMING_PATH = os.environ.get("SILVER_STREAMING_PATH", "/tmp/lake/silver/ventas_streaming")

_builder = (
    SparkSession.builder.appName("ST1630-Lab2b-VerSilver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Solo para mostrar: las ventanas se ven en hora de Colombia, que es la
    # del reloj con el que se lanzan las corridas del productor. En Delta
    # quedan guardadas como timestamps absolutos, sin zona.
    .config("spark.sql.session.timeZone", "America/Bogota")
)
spark = configure_spark_with_delta_pip(_builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(SILVER_STREAMING_PATH)
filas = df.count()
llaves = df.select("window_start", "window_end", "region").distinct().count()

print("=" * 72)
print(f"Silver: {SILVER_STREAMING_PATH}")
print(f"  filas totales                          = {filas}")
print(f"  llaves (window_start, window_end, region) distintas = {llaves}")
print(f"  filas duplicadas por llave             = {filas - llaves}")
print("=" * 72)
(
    df.orderBy(F.col("window_start").desc(), "region")
    .select(
        F.date_format("window_start", "yyyy-MM-dd HH:mm").alias("window_start"),
        F.date_format("window_end", "HH:mm").alias("window_end"),
        "region",
        F.format_number("ventas_totales", 0).alias("ventas_totales"),
        "num_pedidos",
    )
    .show(50, truncate=False)
)
spark.stop()
