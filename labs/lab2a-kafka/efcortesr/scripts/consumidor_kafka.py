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
import sys
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
from pyspark.sql import Row, SparkSession

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_BRONZE_PATH = (
    str(REPO_ROOT / "data" / "local" / "lake" / "bronze" / "pedidos")
    if os.name == "nt"
    else "/tmp/lake/bronze/pedidos"
)

if os.name == "nt":
    local_hadoop_home = REPO_ROOT / "hadoop"
    if "HADOOP_HOME" not in os.environ and (local_hadoop_home / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(local_hadoop_home)
        os.environ["hadoop.home.dir"] = str(local_hadoop_home)
    hadoop_bin = str(local_hadoop_home / "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# ─────────────────────────────────────────────────────────────
# Configuración -- funciona en local sin cambios; las variables de
# entorno permiten apuntar a otro clúster/datalake sin tocar código.
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
BRONZE_PATH = os.environ.get("BRONZE_PATH", DEFAULT_BRONZE_PATH)
TOPIC = "pedidos-ventas"
GROUP_ID = os.environ.get("GROUP_ID", "analytics-group")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1"))
ASSIGN_PARTITION = os.environ.get("ASSIGN_PARTITION")
ASSIGN_PARTITION = int(ASSIGN_PARTITION) if ASSIGN_PARTITION not in (None, "") else None
START_OFFSET = os.environ.get("START_OFFSET")
START_OFFSET = int(START_OFFSET) if START_OFFSET not in (None, "") else None
FAIL_AFTER_MERGE_ON_OFFSET = os.environ.get("FAIL_AFTER_MERGE_ON_OFFSET")
FAIL_AFTER_MERGE_ON_OFFSET = (
    int(FAIL_AFTER_MERGE_ON_OFFSET)
    if FAIL_AFTER_MERGE_ON_OFFSET not in (None, "")
    else None
)

spark_builder = (
    SparkSession.builder.appName("ST1630-Lab2a-Consumidor")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.local.dir", str(REPO_ROOT / "data" / "local" / "spark-tmp"))
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

# ═══════════════════════════════════════════════════════════════
# TODO 2.1 · Configuración del KafkaConsumer
# ═══════════════════════════════════════════════════════════════
# group_id="analytics-group": le da nombre a este consumer group. Sin
# group_id, Kafka no puede rastrear offsets consistentemente para tu
# aplicación -- y un nombre distinto te permitiría tener OTRO grupo
# leyendo el mismo topic de forma completamente independiente (p. ej.
# un grupo "fraude-group" leyendo los mismos mensajes para otro fin).
#
# auto_offset_reset="earliest": el productor YA envió sus 1.000
# mensajes antes de que existiera este consumer group -- si usaras
# "latest", tu consumer solo vería mensajes NUEVOS a partir de ahora y
# no leería nada de lo que el productor ya publicó.
#
# enable_auto_commit=False -- LA DECISIÓN MÁS IMPORTANTE de este
# script. Si la dejaras en True (el default), Kafka commitearía el
# offset automáticamente cada 5 segundos SIN IMPORTAR si ya
# terminaste de procesar ese mensaje. Si tu consumidor se cae justo
# entre ese auto-commit y el MERGE a Bronze, Kafka ya "olvidó" ese
# mensaje -- al reiniciar, retomarías DESPUÉS de él, y ese pedido se
# pierde para siempre. Eso es at-most-once silencioso: nunca te
# enteras de que perdiste datos. Con enable_auto_commit=False, TÚ
# controlas exactamente cuándo Kafka considera "leído" un mensaje --
# y en este script, eso pasa solo después de que el MERGE fue exitoso.
#
# TODO: crea el KafkaConsumer con:
#   - TOPIC como primer argumento posicional
#   - bootstrap_servers=[KAFKA_BOOTSTRAP]
#   - group_id=GROUP_ID
#   - auto_offset_reset="earliest"
#   - enable_auto_commit=False
#   - value_deserializer: función que reciba bytes y devuelva un dict
#     (json.loads(v.decode("utf-8")))
#   - key_deserializer: función que reciba bytes (o None) y devuelva
#     un string (o None)
def crear_consumer():
    """Crea el consumer; permite asignacion manual para pruebas controladas."""
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key is not None else None,
    )

    if ASSIGN_PARTITION is None:
        kafka_consumer.subscribe([TOPIC])
        return kafka_consumer

    topic_partition = TopicPartition(TOPIC, ASSIGN_PARTITION)
    kafka_consumer.assign([topic_partition])
    if START_OFFSET is not None:
        kafka_consumer.seek(topic_partition, START_OFFSET)
    return kafka_consumer


consumer = crear_consumer()


def construir_fila_bronze(mensaje) -> dict:
    """A partir de un ConsumerRecord de kafka-python, arma el dict que
    se va a escribir en Bronze -- el pedido tal cual llegó, más 4
    columnas de trazabilidad. Estas columnas son un patrón de
    producción real: te permiten reconstruir, para cualquier fila de
    Bronze, exactamente de qué topic/partición/offset de Kafka vino --
    útil para debugging y para auditorías de linaje de datos."""
    pedido = dict(mensaje.value)
    # TODO: agrega estas 4 columnas al dict `pedido` antes de retornarlo:
    #   - "_kafka_offset": mensaje.offset
    #   - "_kafka_partition": mensaje.partition
    #   - "_kafka_topic": mensaje.topic
    #   - "_ingested_at": timestamp actual en ISO 8601
    #     (datetime.now(timezone.utc).isoformat())
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
    merge_filas_a_bronze([fila])


def merge_filas_a_bronze(filas: list[dict]):
    """MERGE Delta idempotente para una o varias filas Bronze."""
    df_nuevo = spark.createDataFrame([Row(**fila) for fila in filas])

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


def commit_mensajes(mensajes):
    """Commitea el siguiente offset procesable por particion."""
    offsets = {}
    for mensaje in mensajes:
        topic_partition = TopicPartition(mensaje.topic, mensaje.partition)
        next_offset = mensaje.offset + 1
        current_offset = offsets.get(topic_partition)
        if current_offset is None or next_offset > current_offset.offset:
            offsets[topic_partition] = OffsetAndMetadata(next_offset, None)

    consumer.commit(offsets=offsets)


def seek_primer_offset_no_commiteado(mensajes):
    """Reposiciona cada particion al menor offset del batch fallido."""
    offsets = {}
    for mensaje in mensajes:
        topic_partition = TopicPartition(mensaje.topic, mensaje.partition)
        offsets[topic_partition] = min(
            mensaje.offset,
            offsets.get(topic_partition, mensaje.offset),
        )

    for topic_partition, offset in offsets.items():
        consumer.seek(topic_partition, offset)


# ═══════════════════════════════════════════════════════════════
# TODO 2.2 / 2.3 · Loop principal -- procesar y commitear
# ═══════════════════════════════════════════════════════════════
# Esta es la coreografía completa de at-least-once real:
#
#   1. Leer el mensaje (el for ya te lo da)
#   2. construir_fila_bronze(mensaje)          [dado arriba]
#   3. merge_a_bronze(fila)                     [dado arriba]
#   4. SOLO SI el paso 3 no lanzó excepción: consumer.commit()
#   5. Si el paso 3 falla: NO commitear, loggear el offset que falló
#      con su excepción, y seguir (el mensaje se va a reprocesar la
#      próxima vez que el consumer arranque, exactamente como se
#      espera de at-least-once)
#
# TODO: completa el cuerpo del for con un try/except:
#   try:
#       fila = construir_fila_bronze(mensaje)
#       merge_a_bronze(fila)
#       consumer.commit()  # <- SOLO aquí, después del MERGE exitoso
#       contador_procesados += 1
#       print(f"[OK] offset={mensaje.offset} partition={mensaje.partition} "
#             f"pedido_id={fila['pedido_id']}")
#   except Exception as e:
#       contador_rechazados += 1
#       print(f"[ERROR] offset={mensaje.offset} partition={mensaje.partition} "
#             f"no se commiteó -- se reprocesará. Causa: {e}")
def main():
    contador_procesados = 0
    contador_rechazados = 0

    print(f"Escuchando '{TOPIC}' como grupo '{GROUP_ID}' (bootstrap: {KAFKA_BOOTSTRAP})...")
    print(f"Escribiendo a Bronze en: {BRONZE_PATH}")
    if ASSIGN_PARTITION is not None:
        print(f"Asignacion manual: partition={ASSIGN_PARTITION} start_offset={START_OFFSET}")
    if MAX_MESSAGES > 0:
        print(f"Modo prueba: el consumidor se detendra tras {MAX_MESSAGES} mensajes procesados.")
    if BATCH_SIZE > 1:
        print(f"Modo batch: hasta {BATCH_SIZE} mensajes por MERGE.")
    print("Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).\n")

    if BATCH_SIZE > 1:
        while MAX_MESSAGES == 0 or contador_procesados < MAX_MESSAGES:
            pendientes = MAX_MESSAGES - contador_procesados if MAX_MESSAGES > 0 else BATCH_SIZE
            max_records = min(BATCH_SIZE, pendientes)
            records = consumer.poll(timeout_ms=1000, max_records=max_records)
            mensajes = [
                mensaje
                for partition_records in records.values()
                for mensaje in partition_records
            ]

            if not mensajes:
                continue

            try:
                filas = [construir_fila_bronze(mensaje) for mensaje in mensajes]
                merge_filas_a_bronze(filas)

                if any(mensaje.offset == FAIL_AFTER_MERGE_ON_OFFSET for mensaje in mensajes):
                    raise RuntimeError("Fallo controlado despues del MERGE y antes del commit")

                commit_mensajes(mensajes)
                contador_procesados += len(mensajes)
                ultimo = mensajes[-1]
                print(f"[OK] batch={len(mensajes)} ultimo_offset={ultimo.offset} "
                      f"ultima_partition={ultimo.partition} procesados={contador_procesados}")
            except Exception as e:
                contador_rechazados += len(mensajes)
                seek_primer_offset_no_commiteado(mensajes)
                primero = mensajes[0]
                print(f"[ERROR] batch={len(mensajes)} primer_offset={primero.offset} "
                      f"partition={primero.partition} no se commiteo -- se reprocesara. "
                      f"Causa: {e}")
                break

        print(f"\nProcesados: {contador_procesados}  Rechazados (sin commit): {contador_rechazados}")
        return

    for mensaje in consumer:
        try:
            fila = construir_fila_bronze(mensaje)
            merge_a_bronze(fila)

            if mensaje.offset == FAIL_AFTER_MERGE_ON_OFFSET:
                raise RuntimeError("Fallo controlado despues del MERGE y antes del commit")

            commit_mensajes([mensaje])
            contador_procesados += 1
            print(f"[OK] offset={mensaje.offset} partition={mensaje.partition} "
                  f"pedido_id={fila['pedido_id']} commit={mensaje.offset + 1}")

            if MAX_MESSAGES > 0 and contador_procesados >= MAX_MESSAGES:
                break
        except Exception as e:
            contador_rechazados += 1
            seek_primer_offset_no_commiteado([mensaje])
            print(f"[ERROR] offset={mensaje.offset} partition={mensaje.partition} "
                  f"no se commiteo -- se reprocesara. Causa: {e}")
            break

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
