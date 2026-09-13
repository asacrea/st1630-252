"""Cuenta registros en Bronze Delta para validar el Lab 2a."""

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_BRONZE_PATH = (
    str(REPO_ROOT / "data" / "local" / "lake" / "bronze" / "pedidos")
    if os.name == "nt"
    else "/tmp/lake/bronze/pedidos"
)
BRONZE_PATH = os.environ.get("BRONZE_PATH", DEFAULT_BRONZE_PATH)

if os.name == "nt":
    hadoop_home = REPO_ROOT / "hadoop"
    hadoop_bin = str(hadoop_home / "bin")
    os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
    os.environ.setdefault("hadoop.home.dir", str(hadoop_home))
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

spark_builder = (
    SparkSession.builder.appName("ST1630-Lab2a-ContarBronze")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.local.dir", str(REPO_ROOT / "data" / "local" / "spark-tmp"))
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

try:
    df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"BRONZE_PATH={BRONZE_PATH}")
    print(f"COUNT={df.count()}")
    df.groupBy("_kafka_partition").count().orderBy("_kafka_partition").show(truncate=False)
finally:
    spark.stop()
