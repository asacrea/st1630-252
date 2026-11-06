"""05_export_csv_10k.py — Lab 1b (Parte 5.2, preparación del benchmark)

Exporta una muestra de 10.000 filas de Silver a CSV sin particionar
(un solo archivo, con header) en s3://<bucket>/benchmark/csv_10k/, con
las columnas del DDL de `benchmark_csv_10k` de 04_athena_benchmark.py.

Uso (en EMR, con los JAR de Delta del clúster):
    spark-submit --jars /usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar 05_export_csv_10k.py
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ST1630-Lab1b-ExportCSV10k")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

BUCKET = "st1630-jjgomezv2-2026"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
CSV_10K = f"s3a://{BUCKET}/benchmark/csv_10k/"

COLUMNAS = [
    "pedido_id", "fecha", "region", "canal", "categoria", "producto",
    "cantidad", "precio_unit", "total_silver",
]

muestra = spark.read.format("delta").load(SILVER).select(*COLUMNAS).limit(10_000)
muestra.coalesce(1).write.mode("overwrite").option("header", "true").csv(CSV_10K)

print(f"Muestra exportada: {spark.read.option('header', 'true').csv(CSV_10K).count():,} filas en {CSV_10K}")
spark.stop()
