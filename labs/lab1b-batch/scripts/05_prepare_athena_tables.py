"""05_prepare_athena_tables.py - Lab 1b

Exports the current Delta tables to clean Parquet/CSV locations that Athena can
read reliably through Glue external tables.
"""

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("ST1630-Lab1b-Prepare-Athena").getOrCreate()

BUCKET = "st1630-efcortesr-2026"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
GOLD = f"s3a://{BUCKET}/gold/kpis"
ATHENA = f"s3a://{BUCKET}/athena"
BENCHMARK = f"s3a://{BUCKET}/benchmark"


def export_delta_to_parquet(source: str, target: str) -> None:
    df = spark.read.format("delta").load(source)
    rows = df.count()
    print(f"Exportando {source} -> {target} ({rows:,} filas)")
    df.coalesce(1).write.mode("overwrite").parquet(target)


export_delta_to_parquet(
    f"{GOLD}/ventas_region_fecha",
    f"{ATHENA}/gold_ventas_region_fecha",
)
export_delta_to_parquet(
    f"{GOLD}/top_productos_categoria",
    f"{ATHENA}/gold_top_productos_categoria",
)
export_delta_to_parquet(
    f"{GOLD}/cohortes_canal_pago",
    f"{ATHENA}/gold_cohortes_canal_pago",
)

df_silver = spark.read.format("delta").load(SILVER)
csv_sample = df_silver.select(
    "pedido_id",
    "fecha",
    "region",
    "canal",
    "categoria",
    "producto",
    "cantidad",
    "precio_unit",
    "total_silver",
).limit(10000)

print(f"Exportando muestra CSV benchmark -> {BENCHMARK}/csv_10k/ ({csv_sample.count():,} filas)")
(
    csv_sample.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{BENCHMARK}/csv_10k/")
)

print("Tablas base para Athena exportadas correctamente")
spark.stop()
