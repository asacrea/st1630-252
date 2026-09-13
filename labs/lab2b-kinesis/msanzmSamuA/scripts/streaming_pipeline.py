"""streaming_pipeline.py — Lab 2b (ST1630-2026-2, S7)

Spark Structured Streaming sobre el mismo topic de pedidos del Lab 2a
("pedidos-ventas"), agregando ventas por región en ventanas de tiempo
con watermark, y escribiendo el resultado a Delta con un patrón
foreachBatch + MERGE (idempotente, mismo espíritu que Lab 1b/2a).

La Parte 4 (opcional) reemplaza Kafka por Kinesis Data Streams como
fuente -- la función crear_stream_kinesis() ya viene resuelta como
referencia de cómo se ve la MISMA lógica de streaming con OTRO
transporte. Ese es el punto pedagógico central de este script: todo lo
que pasa DESPUÉS de crear_stream_*() (ventana, watermark, sink) es
idéntico sin importar el origen.

Este script tiene bloques marcados con # TODO -- son las Partes 1, 2 y
3, el núcleo nuevo de esta semana. La Parte 4 (Kinesis) viene dada
porque su objetivo es que COMPARES, no que reimplementes desde cero
una decisión que ya tomaste en Kafka.

Uso:
    # Fuente Kafka (por defecto -- requiere el topic del Lab 2a con datos)
    python3 streaming_pipeline.py
    # Fuente Kinesis (Parte 4, opcional)
    STREAM_SOURCE=kinesis python3 streaming_pipeline.py
"""

import os
import sys

# Configuración de compatibilidad Windows para Hadoop / winutils
if os.name == "nt":
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    hadoop_bin = os.path.join(os.environ["HADOOP_HOME"], "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
)

# ─────────────────────────────────────────────────────────────
# Configuración -- funciona en local sin cambios; las variables de
# entorno permiten apuntar a otra fuente/datalake sin tocar código.
# ─────────────────────────────────────────────────────────────
STREAM_SOURCE = os.environ.get("STREAM_SOURCE", "kafka")  # "kafka" | "kinesis"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "pedidos-ventas")

KINESIS_STREAM = os.environ.get("KINESIS_STREAM", "pedidos-ventas-kinesis")
KINESIS_REGION = os.environ.get("KINESIS_REGION", "us-east-1")

SILVER_STREAMING_PATH = os.environ.get("SILVER_STREAMING_PATH", "/tmp/lake/silver/ventas_streaming")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", f"/tmp/lake/checkpoints/lab2b-{STREAM_SOURCE}")

# Mismo schema que publica ../../lab2a-kafka/scripts/productor_kafka.py
PEDIDO_SCHEMA = StructType([
    StructField("pedido_id", StringType()),
    StructField("fecha", StringType()),
    StructField("region", StringType()),
    StructField("categoria", StringType()),
    StructField("producto", StringType()),
    StructField("cantidad", IntegerType()),
    StructField("precio_unit", DoubleType()),
    StructField("total", DoubleType()),
    StructField("canal", StringType()),
    StructField("metodo_pago", StringType()),
    StructField("devuelto", BooleanType()),
])

_builder = (
    SparkSession.builder.appName("ST1630-Lab2b-Streaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
# Resolución dinámica de la versión de Scala según PySpark (2.13 para Spark 4.x, 2.12 para Spark 3.x)
_scala_version = "2.13" if pyspark.__version__.startswith("4.") else "2.12"
_kafka_connector = f"org.apache.spark:spark-sql-kafka-0-10_{_scala_version}:{pyspark.__version__}"
spark = configure_spark_with_delta_pip(_builder, extra_packages=[_kafka_connector]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# ═══════════════════════════════════════════════════════════════
# TODO Parte 1 · Leer de Kafka con Structured Streaming
# ═══════════════════════════════════════════════════════════════
def crear_stream_kafka(spark) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), PEDIDO_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_time"),
        )
        .select("d.*", "kafka_time")
    )


# ═══════════════════════════════════════════════════════════════
# Parte 4 (OPCIONAL, evaluable) · Leer de Kinesis Data Streams
# ═══════════════════════════════════════════════════════════════
def crear_stream_kinesis(spark) -> DataFrame:
    return (
        spark.readStream
        .format("aws-kinesis")
        .option("kinesis.region", KINESIS_REGION)
        .option("kinesis.streamName", KINESIS_STREAM)
        .option("kinesis.consumerType", "GetRecords")
        .option("kinesis.startingposition", "TRIM_HORIZON")
        .load()
        .select(
            F.from_json(F.col("data").cast("string"), PEDIDO_SCHEMA).alias("d"),
            F.col("approximateArrivalTimestamp").alias("kafka_time"),
        )
        .select("d.*", "kafka_time")
    )


# ═══════════════════════════════════════════════════════════════
# TODO Parte 2 · Ventana de tiempo + watermark
# ═══════════════════════════════════════════════════════════════
def aplicar_ventana(df: DataFrame) -> DataFrame:
    return (
        df
        .withWatermark("kafka_time", "10 minutes")
        .groupBy(
            F.window(F.col("kafka_time"), "5 minutes"),
            F.col("region"),
        )
        .agg(
            F.sum("total").alias("ventas_totales"),
            F.count("pedido_id").alias("num_pedidos"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("region"),
            F.col("ventas_totales"),
            F.col("num_pedidos"),
        )
    )


# ═══════════════════════════════════════════════════════════════
# TODO Parte 3 · Sink foreachBatch + MERGE (idempotente)
# ═══════════════════════════════════════════════════════════════
def escribir_batch(df_micro_batch: DataFrame, id_batch: int) -> None:
    if df_micro_batch.isEmpty():
        return

    # Si la tabla en SILVER_STREAMING_PATH no existe todavía, primera escritura
    if not DeltaTable.isDeltaTable(spark, SILVER_STREAMING_PATH):
        (
            df_micro_batch.write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_STREAMING_PATH)
        )
    else:
        # MERGE por (window_start, window_end, region) idempotente
        tabla_delta = DeltaTable.forPath(spark, SILVER_STREAMING_PATH)
        (
            tabla_delta.alias("existente")
            .merge(
                df_micro_batch.alias("nuevo"),
                "existente.window_start = nuevo.window_start AND "
                "existente.window_end = nuevo.window_end AND "
                "existente.region = nuevo.region"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def main():
    if STREAM_SOURCE == "kinesis":
        print(f"Fuente: Kinesis ({KINESIS_STREAM}, región {KINESIS_REGION})")
        df_raw = crear_stream_kinesis(spark)
    else:
        print(f"Fuente: Kafka ({KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP})")
        df_raw = crear_stream_kafka(spark)

    df_ventanas = aplicar_ventana(df_raw)

    print(f"Escribiendo agregados a: {SILVER_STREAMING_PATH}")
    print(f"Checkpoint en: {CHECKPOINT_PATH}")

    query = (
        df_ventanas.writeStream
        .outputMode("update")
        .foreachBatch(escribir_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDetenido por el usuario (Ctrl+C).")
    finally:
        spark.stop()
