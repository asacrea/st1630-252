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

Prerequisito de entorno: pip install pyspark delta-spark (sin fijar
versión -- ver ../README.md, Prerequisito). El conector de Kafka para
Structured Streaming (`spark-sql-kafka-0-10`) NO viene incluido con
pyspark -- este script lo agrega automáticamente vía
`configure_spark_with_delta_pip(..., extra_packages=[...])`, usando la
misma versión de pyspark que tengas instalada, así que no necesitas
pasar `--packages` a mano para la fuente Kafka.

Qué puedes delegar: boilerplate de sintaxis de Structured Streaming si
te trabas. Qué NO puedes delegar: el tamaño de ventana y el watermark
que elijas, y por qué el MERGE del sink usa una llave distinta a la
del Lab 2a -- ver ../README.md, "Bitácora de delegación".
"""

import os

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

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
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
# configure_spark_with_delta_pip lee la versión de delta-spark instalada
# y resuelve su JAR automáticamente -- sin esto, .format("delta") falla.
# extra_packages agrega TAMBIÉN el conector de Kafka para Structured
# Streaming (spark-sql-kafka-0-10) -- sin él, .format("kafka") falla
# con "Failed to find data source: kafka" apenas llamas a .load(). La
# versión tiene que coincidir exactamente con tu pyspark instalado.
_kafka_connector = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
spark = configure_spark_with_delta_pip(_builder, extra_packages=[_kafka_connector]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# ═══════════════════════════════════════════════════════════════
# TODO Parte 1 · Leer de Kafka con Structured Streaming
# ═══════════════════════════════════════════════════════════════
# Diferencia clave con el Lab 2a: ahí usabas KafkaConsumer (la librería
# kafka-python) y un loop `for mensaje in consumer` que tú controlabas
# mensaje a mensaje. Aquí Spark administra la lectura por ti -- le
# describes DE DÓNDE leer y Spark se encarga de pedir batches de
# mensajes, rastrear su propio progreso (ver TODO Parte 3, sección de
# checkpoint) y reintentar si algo falla.
#
# `spark.readStream` (no `spark.read`) es lo que convierte esto en una
# fuente de streaming en vez de una lectura batch de una sola vez.
#
# TODO: construye el DataFrame de streaming:
#   1. spark.readStream.format("kafka")
#   2. .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#   3. .option("subscribe", KAFKA_TOPIC)
#   4. .option("startingOffsets", "earliest")  # el productor del Lab 2a
#      ya escribió antes -- mismo razonamiento que auto_offset_reset
#      en el Lab 2a: con "latest" no verías nada de lo ya publicado.
#   5. .load()
#
# El DataFrame que devuelve .load() trae columnas fijas de Kafka:
# key, value, topic, partition, offset, timestamp, timestampType.
# `value` es binario (bytes) -- tienes que decodificarlo y parsearlo
# como JSON con el PEDIDO_SCHEMA. `timestamp` es el timestamp que
# Kafka le puso al mensaje (por defecto, el momento en que el producer
# lo envió) -- ESE es el "tiempo de evento" que vas a usar para las
# ventanas de la Parte 2, no el momento en que Spark lo procesa.
#
#   6. .select(
#          F.from_json(F.col("value").cast("string"), PEDIDO_SCHEMA).alias("d"),
#          F.col("timestamp").alias("kafka_time"),
#      )
#   7. .select("d.*", "kafka_time")
def crear_stream_kafka(spark) -> DataFrame:
    raise NotImplementedError("TODO Parte 1: construye el readStream de Kafka (ver especificación arriba)")


# ═══════════════════════════════════════════════════════════════
# Parte 4 (OPCIONAL, evaluable) · Leer de Kinesis Data Streams
# ═══════════════════════════════════════════════════════════════
# Alternativa a crear_stream_kafka() usando Kinesis como source.
# La lógica de ventanas, watermark y sink (Partes 2 y 3) es IDÉNTICA
# -- solo cambia esta función. Por eso viene dada: el objetivo de esta
# parte es que COMPARES contra tu propia crear_stream_kafka(), no que
# la reimplementes desde cero.
#
# Requiere el conector de Kinesis para Spark. Al correr este script
# con STREAM_SOURCE=kinesis:
#   spark-submit \
#     --packages com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0 \
#     streaming_pipeline.py
def crear_stream_kinesis(spark) -> DataFrame:
    return (
        spark.readStream
        .format("aws-kinesis")
        .option("kinesis.region", KINESIS_REGION)
        .option("kinesis.streamName", KINESIS_STREAM)
        .option("kinesis.consumerType", "GetRecords")
        # TRIM_HORIZON = earliest (leer desde el inicio del stream)
        # LATEST = solo mensajes nuevos desde ahora -- mismo par de
        # opciones que auto_offset_reset en Kafka/Lab 2a.
        .option("kinesis.startingposition", "TRIM_HORIZON")
        .load()
        .select(
            # En Kinesis el payload viene en la columna "data" (bytes)
            # -- en Kafka era "value". Todo lo demás del pipeline no
            # necesita saber esta diferencia de nombres.
            F.from_json(F.col("data").cast("string"), PEDIDO_SCHEMA).alias("d"),
            F.col("approximateArrivalTimestamp").alias("kafka_time"),
        )
        .select("d.*", "kafka_time")
    )
    # Nota de diseño: renombramos approximateArrivalTimestamp también a
    # "kafka_time" (no "kinesis_time") a propósito -- así
    # aplicar_ventana() y escribir_batch() de abajo no necesitan saber
    # de qué fuente vino el DataFrame. Es la forma más directa de
    # demostrar en código lo que dice el enunciado: "la lógica de
    # ventanas, watermark y sink es idéntica, solo cambia el source".


# ═══════════════════════════════════════════════════════════════
# TODO Parte 2 · Ventana de tiempo + watermark
# ═══════════════════════════════════════════════════════════════
# Watermark: le dice a Spark "no esperes datos con más de X de retraso
# respecto al evento más reciente que ya viste". Sin watermark, Spark
# tendría que guardar el estado de TODAS las ventanas abiertas para
# siempre (memoria ilimitada) -- con watermark, puede cerrar y liberar
# el estado de una ventana una vez pasa ese umbral de tolerancia.
#
# Trade-off: un watermark corto libera memoria rápido pero descarta
# datos que lleguen más tarde que eso (llegan, pero Spark ya cerró esa
# ventana y los ignora). Un watermark largo tolera más retraso pero
# mantiene más estado en memoria por más tiempo.
#
# TODO: sobre el DataFrame `df` (que ya tiene la columna "kafka_time"
# de tiempo de evento):
#   1. .withWatermark("kafka_time", "<tu umbral, ej. '10 minutes'>")
#   2. .groupBy(
#          F.window(F.col("kafka_time"), "<tu tamaño de ventana, ej. '5 minutes'>"),
#          F.col("region"),
#      )
#   3. .agg(
#          F.sum("total").alias("ventas_totales"),
#          F.count("pedido_id").alias("num_pedidos"),
#      )
#   4. Aplana la columna "window" (un struct con "start"/"end") a dos
#      columnas planas "window_start" y "window_end" -- las vas a
#      necesitar como parte de la llave del MERGE en la Parte 3.
def aplicar_ventana(df: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO Parte 2: implementa watermark + groupBy(window(...), region) + agg")


# ═══════════════════════════════════════════════════════════════
# TODO Parte 3 · Sink foreachBatch + MERGE (idempotente)
# ═══════════════════════════════════════════════════════════════
# Con outputMode("update"), Spark te entrega en cada micro-batch SOLO
# las filas de ventana que cambiaron desde el último trigger -- una
# ventana puede recibir varias actualizaciones (más pedidos llegando)
# antes de que el watermark la cierre definitivamente. Por eso el sink
# tiene que ser un UPSERT, no un append: si solo hicieras append,
# tendrías una fila vieja Y una nueva para la misma ventana cada vez
# que se actualiza, en vez de una sola fila con el valor más reciente.
#
# La llave de este MERGE es (window_start, window_end, region) -- NO
# pedido_id como en el Lab 2a. Tiene sentido: ya no estás mergeando
# pedidos individuales, sino agregados por ventana. Dos micro-batches
# que reportan la MISMA ventana+región deben pisarse (UPDATE), no
# sumarse ni duplicarse.
#
# TODO: dentro de esta función (que Spark llama una vez POR
# micro-batch, con el DataFrame ya materializado -- por eso aquí SÍ
# puedes usar operaciones batch normales como DeltaTable.merge()):
#   1. Si la tabla en SILVER_STREAMING_PATH no existe todavía:
#      escribir df_micro_batch directo (mode="overwrite", primera vez).
#   2. Si ya existe: DeltaTable.forPath(...).alias("existente")
#      .merge(df_micro_batch.alias("nuevo"),
#             "existente.window_start = nuevo.window_start AND "
#             "existente.window_end = nuevo.window_end AND "
#             "existente.region = nuevo.region")
#      .whenMatchedUpdateAll()
#      .whenNotMatchedInsertAll()
#      .execute()
def escribir_batch(df_micro_batch: DataFrame, id_batch: int) -> None:
    raise NotImplementedError("TODO Parte 3: implementa el MERGE por (window_start, window_end, region)")


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
