"""01_bronze.py — Lab 1b (ST1630-2026-2, S5-S6)

Ingesta cruda a Bronze: el CSV entra a Delta Lake TAL CUAL viene, sin
limpiar ni tipar nada. Bronze es la capa de "verdad del origen" -- si
algo sale mal en Silver o Gold, siempre puedes volver a Bronze y
reprocesar, porque Bronze nunca se sobreescribe con datos transformados.

Este script tiene bloques marcados con # TODO -- ESE es tu trabajo.
Todo lo demás (imports, rutas, la verificación del final) ya está
resuelto para que puedas concentrarte en las partes que de verdad
enseñan algo nuevo esta semana.

Uso:
    python3 01_bronze.py
    # o: spark-submit 01_bronze.py

Prerequisito de entorno: pip install pyspark delta-spark==2.4.0 (esa
versión de Delta es la compatible con Spark 3.4.1, la que trae el
clúster EMR 6.15.0 del Lab 1a -- si tu clúster corre otra versión de
Spark, ajusta la versión de delta-spark según la tabla de
compatibilidad de delta.io antes de instalar).

Qué puedes delegar: dudas de sintaxis puntuales si te trabas en un
TODO (¿cómo se llama el método para X?). Qué NO puedes delegar: por
qué el schema es 100% string -- tienes que poder explicarlo con tus
propias palabras (te lo preguntamos en la defensa del lab).
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# configure_spark_with_delta_pip lee la versión de delta-spark que
# tengas instalada (pip install delta-spark==2.4.0) y le dice a Spark
# dónde descargar/encontrar el JAR de Delta que le corresponde -- sin
# esto, `.format("delta")` falla con
# "Failed to find the data source: delta" apenas intentas leer/escribir.
_builder = (
    SparkSession.builder.appName("ST1630-Lab1b-Bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(_builder).getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "32")  # clúster del curso: 4 executors x 8 cores

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-efcortesr-2026"  # Mismo bucket del Lab 1a
RAW = f"s3a://{BUCKET}/raw/ventas_colombia_raw.csv"
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
# ─────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════
# TODO 1 · Schema explícito -- TODOS los campos como StringType
# ═══════════════════════════════════════════════════════════════
# Justificación (esto SÍ te lo damos resuelto -- entiéndelo antes de
# construir el schema): Bronze recibe el dato tal cual el sistema de
# origen lo entregó, sin asumir ningún tipo. Si aquí ya castearas
# 'total' a double, por ejemplo, Spark tendría que decidir qué hacer
# con un valor corrupto como una celda vacía o con letras -- y esa
# decisión (¿null? ¿0? ¿falla el job?) es una transformación, no una
# ingesta. La conversión de tipos es responsabilidad exclusiva de
# Silver, donde SÍ hay contexto de negocio para decidir cómo tratar
# cada caso raro.
#
# Las columnas del CSV crudo, EN EL ORDEN REAL en que aparecen en
# ../datos/ventas_colombia_raw.csv (verifica tú mismo con
# `head -1 ventas_colombia_raw.csv` -- no asumas el orden "lógico" de
# las columnas, confirma el real. Aquí region/canal/vendedor_id
# terminan al final porque así las dejó gen_dataset.py):
#   pedido_id, fecha, categoria, producto, cantidad, precio_unit,
#   total, email_cliente, metodo_pago, devuelto, calificacion,
#   region, canal, vendedor_id
#
# TODO: construye BRONZE_SCHEMA como un StructType con un
# StructField(nombre, StringType(), True) por cada una de las 14
# columnas de arriba, en ese mismo orden.
BRONZE_SCHEMA = StructType([
    StructField("pedido_id", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("producto", StringType(), True),
    StructField("cantidad", StringType(), True),
    StructField("precio_unit", StringType(), True),
    StructField("total", StringType(), True),
    StructField("email_cliente", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("devuelto", StringType(), True),
    StructField("calificacion", StringType(), True),
    StructField("region", StringType(), True),
    StructField("canal", StringType(), True),
    StructField("vendedor_id", StringType(), True),
])

# ═══════════════════════════════════════════════════════════════
# TODO 2 · Lectura del CSV con schema explícito
# ═══════════════════════════════════════════════════════════════
# TODO: usa spark.read, con .option("header", "true"), .schema(BRONZE_SCHEMA)
# y .csv(RAW) para leer el archivo crudo en df_raw.
#
# Además, agrega .option("enforceSchema", "false"): por defecto Spark
# aplica el schema por POSICIÓN e ignora si los nombres del header
# realmente coinciden -- si el orden de columnas de arriba está mal
# (a alguien se le pasó, o el CSV cambió), terminas con datos
# silenciosamente mal etiquetados sin ningún error. Con
# enforceSchema=false, Spark valida el header contra tu schema y
# falla fuerte y claro si no coinciden, en vez de corromper todo el
# pipeline sin que te enteres.
#
# Clasificación: → [NARROW ✅ / WIDE ❌] -- justifica en un comentario
# por qué (pista: ¿esta lectura necesita comparar o mover datos entre
# particiones para poder aplicarle el schema?).
# Clasificacion: NARROW. El schema se aplica por particion de entrada;
# no requiere comparar filas ni mover datos entre executors.
df_raw = (
    spark.read
    .option("header", "true")
    .option("nullValue", "")
    .schema(BRONZE_SCHEMA)
    .csv(RAW)
)

# ═══════════════════════════════════════════════════════════════
# TODO 3 · Columnas de auditoría
# ═══════════════════════════════════════════════════════════════
# TODO: a partir de df_raw, agrega dos columnas con withColumn():
#   - "_ingested_at": el timestamp de cuándo se corrió esta ingesta
#     (busca la función de pyspark.sql.functions que da la hora actual)
#   - "_source_file": de qué archivo físico vino cada fila
#     (busca la función que expone el nombre del archivo de origen)
#
# Clasificación: → [NARROW ✅ / WIDE ❌] -- justifica.
# Clasificacion: NARROW. Ambas columnas se calculan fila a fila.
df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# ═══════════════════════════════════════════════════════════════
# TODO 4 · Escritura a Delta en modo append
# ═══════════════════════════════════════════════════════════════
# TODO: escribe df_bronze a la ruta BRONZE en formato "delta", modo
# "append" (NO "overwrite" -- Bronze acumula, nunca reemplaza).
#
# Clasificación: → [NARROW ✅ / WIDE ❌] -- justifica, y compara mentalmente
# contra el MERGE que vas a escribir en 02_silver.py (Parte 3.7): ¿por
# qué ESTA escritura no necesita comparar contra lo que ya existe en
# la tabla, y esa sí?
# Clasificacion: NARROW para esta escritura append. No compara contra
# datos existentes; solo escribe nuevos archivos y registra el commit.
(
    df_bronze.write
    .format("delta")
    .mode("append")
    .save(BRONZE)
)

print(f"Bronze escrito en: {BRONZE}")

# ═══════════════════════════════════════════════════════════════
# Verificación: los problemas del raw SÍ deben estar en Bronze
# (esta parte ya está resuelta -- es tu "examen" automático de que los
# TODOs de arriba quedaron bien).
# ═══════════════════════════════════════════════════════════════
# Si Bronze estuviera limpio en este punto, algo se transformó de más
# -- eso sería un error de diseño, no una mejora.
df_check = spark.read.format("delta").load(BRONZE)

n_total = df_check.count()
n_pedido_null = df_check.filter(F.col("pedido_id").isNull()).count()
n_total_null = df_check.filter(
    F.col("total").isNull()
    | (F.col("total") == "")
    | (F.lower(F.col("total")) == "null")
    | F.col("total").cast("double").isNull()
).count()

# WIDE ❌ Exchange: dropDuplicates() sobre TODAS las columnas necesita
# que Spark calcule un hash de la fila completa y reparticione por ese
# hash, para que dos filas idénticas -- que pudieron haber llegado en
# particiones distintas del archivo original -- terminen comparándose
# en el mismo executor. Eso es un shuffle real (shuffle write local +
# shuffle read por red), con su propio nodo Exchange en el plan físico
# -- lo vas a confirmar tú mismo en Spark UI en la Parte 3.8 del lab.
n_dup = n_total - df_check.dropDuplicates(BRONZE_SCHEMA.fieldNames()).count()

print(f"\n=== Verificación Bronze (deben aparecer los problemas del raw) ===")
print(f"Filas totales: {n_total:,}")
print(f"pedido_id nulos: {n_pedido_null:,} (debe ser > 0)")
print(f"total nulos: {n_total_null:,} (debe ser > 0)")
print(f"Duplicados exactos: {n_dup:,} (debe ser > 0)")
assert n_pedido_null > 0 and n_total_null > 0 and n_dup > 0, (
    "Bronze no debería estar limpio -- revisa tus TODOs, probablemente "
    "se te coló un filtro o un cast antes de escribir."
)

# ── Inspeccionar el _delta_log ──────────────────────────────────
# Cada escritura a una tabla Delta genera un archivo JSON de commit en
# <ruta_tabla>/_delta_log/00000000000000000000.json (el primero),
# 00000000000000000001.json (el segundo), etc. Ese JSON es la fuente
# de verdad de Delta Lake: no es un índice derivado, ES la definición
# de qué archivos Parquet componen la tabla en cada versión.
print(f"""
=== Cómo inspeccionar el primer commit de _delta_log ===
aws s3 cp {BRONZE}/_delta_log/00000000000000000000.json - | python3 -m json.tool | head -50

Busca estas líneas dentro del JSON y anota en tu bitácora qué
representa cada una:
  - "commitInfo"  -> ¿quién hizo el commit? ¿cuándo? ¿con qué operación?
  - "metaData"    -> ¿coincide el schema con tu BRONZE_SCHEMA + las 2
                     columnas de auditoría del TODO 3?
  - "add"         -> ¿cuántos archivos Parquet agregó este commit?
""")

spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1
