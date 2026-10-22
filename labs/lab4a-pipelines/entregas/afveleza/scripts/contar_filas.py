"""Conteos de Bronze / Silver / Gold para evidenciar idempotencia (Lab 4a).

El script se copia a datos/ porque esa carpeta es la que el contenedor
monta (solo lectura) en /opt/airflow/data/raw.

Uso (desde labs/lab4a-pipelines):
    docker compose exec airflow-scheduler python /opt/airflow/data/raw/contar_filas.py
"""
import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

OUT = "/opt/airflow/data/output"
BRONZE = f"{OUT}/bronze/ventas_partitioned"
SILVER = f"{OUT}/silver/ventas"
GOLD_REGION = f"{OUT}/gold/ventas_por_region"
GOLD_TOP = f"{OUT}/gold/top_productos"

builder = (
    SparkSession.builder.appName("lab4a-conteos")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n===== BRONZE (filas por particion) =====")
total_bronze = 0
for carpeta in sorted(os.listdir(BRONZE)):
    if carpeta.startswith("fecha="):
        n = spark.read.parquet(f"{BRONZE}/{carpeta}").count()
        total_bronze += n
        print(f"  {carpeta}: {n}")
print(f"  TOTAL BRONZE: {total_bronze}")

print("\n===== SILVER =====")
silver = spark.read.format("delta").load(SILVER)
print(f"  TOTAL SILVER: {silver.count()}")
print(f"  pedido_id distintos: {silver.select('pedido_id').distinct().count()}")
silver.groupBy("fecha").count().orderBy("fecha").show()

print("===== SILVER: historial Delta (versiones) =====")
DeltaTable.forPath(spark, SILVER).history().select("version", "timestamp", "operation").orderBy("version").show(truncate=False)

print("===== GOLD: ventas_por_region =====")
gold_region = spark.read.format("delta").load(GOLD_REGION)
gold_region.orderBy("region").show()
print(f"  suma n_transacciones: {gold_region.agg({'n_transacciones': 'sum'}).first()[0]}")

print("\n===== GOLD: top_productos =====")
gold_top = spark.read.format("delta").load(GOLD_TOP)
print(f"  filas: {gold_top.count()}")
gold_top.orderBy("total_ventas", ascending=False).show(20, truncate=False)

# Si Gold se recalcula completo, cada corrida de build_gold deja una
# version WRITE (overwrite), nunca MERGE ni append.
for nombre, ruta in [("ventas_por_region", GOLD_REGION), ("top_productos", GOLD_TOP)]:
    print(f"===== GOLD {nombre}: historial Delta =====")
    (DeltaTable.forPath(spark, ruta).history()
        .select("version", "timestamp", "operation", "operationParameters.mode")
        .orderBy("version").show(50, truncate=False))

spark.stop()
