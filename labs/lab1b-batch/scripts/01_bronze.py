"""01_bronze.py — Lab 1b (ST1630-2026-2, S5-S6)

Ingesta cruda a Bronze: el CSV entra a Delta Lake TAL CUAL viene, sin
limpiar ni tipar nada. Bronze es la capa de "verdad del origen" -- si
algo sale mal en Silver o Gold, siempre puedes volver a Bronze y
reprocesar, porque Bronze nunca se sobreescribe con datos transformados.

Uso:
    spark-submit 01_bronze.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("ST1630-Lab1b-Bronze").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "32")  # clúster del curso: 4 executors x 8 cores

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-lemorenog-2026"  # EDITAR: el mismo bucket del Lab 1a
RAW = f"s3a://{BUCKET}/raw/ventas_colombia_raw.csv"
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
# ─────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════
# TODO 1 · Schema explícito -- TODOS los campos como StringType
# ═══════════════════════════════════════════════════════════════
# Bronze recibe el dato tal cual el sistema de origen lo entregó, sin
# asumir ningún tipo. Castear aquí (p. ej. 'total' a double) obligaría a
# Spark a decidir qué hacer con las celdas corruptas -- y esa decisión
# (¿null? ¿0? ¿falla el job?) es una transformación, no una ingesta.
# Un schema explícito además evita el inferSchema, que gastaría un pase
# completo sobre el archivo y podría inferir tipos distintos si mañana
# llega un lote con datos más sucios (esquema no determinista).
BRONZE_SCHEMA = StructType([
    # ORDEN REAL del header de ventas_colombia_raw.csv. Con .schema() +
    # header=true, Spark mapea POR POSICIÓN e ignora los nombres del
    # archivo: si este orden no calza con el CSV, las columnas quedan
    # etiquetadas mal en silencio (no falla, solo miente).
    StructField("pedido_id",     StringType(), True),
    StructField("fecha",         StringType(), True),
    StructField("categoria",     StringType(), True),
    StructField("producto",      StringType(), True),
    StructField("cantidad",      StringType(), True),
    StructField("precio_unit",   StringType(), True),
    StructField("total",         StringType(), True),
    StructField("email_cliente", StringType(), True),
    StructField("metodo_pago",   StringType(), True),
    StructField("devuelto",      StringType(), True),
    StructField("calificacion",  StringType(), True),
    StructField("region",        StringType(), True),
    StructField("canal",         StringType(), True),
    StructField("vendedor_id",   StringType(), True),
])

# ═══════════════════════════════════════════════════════════════
# TODO 2 · Lectura del CSV con schema explícito
# ═══════════════════════════════════════════════════════════════
# Clasificación: → NARROW ✅
# Cada split del CSV se lee y se parsea de forma independiente en su
# propio task: aplicar el schema es una operación fila a fila (partir la
# línea por comas y etiquetar cada campo), no necesita conocer ninguna
# otra fila. Ninguna partición tiene que ver lo que hay en las demás, así
# que no hay Exchange en el plan físico -- solo un FileScan.
df_raw = (
    spark.read
    .option("header", "true")
    .schema(BRONZE_SCHEMA)
    .csv(RAW)
)

# ═══════════════════════════════════════════════════════════════
# TODO 3 · Columnas de auditoría
# ═══════════════════════════════════════════════════════════════
# Clasificación: → NARROW ✅
# Son dos proyecciones puras: cada fila de salida se calcula únicamente a
# partir de esa misma fila de entrada (relación 1:1 entre partición de
# entrada y de salida). current_timestamp() se evalúa una sola vez por
# query -- todas las filas del mismo run comparten el timestamp, que es
# justo lo que quieres para poder decir "este lote entró a esta hora".
# input_file_name() lo resuelve cada task desde los metadatos de su
# propio split: tampoco requiere mover datos.
df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# ═══════════════════════════════════════════════════════════════
# TODO 4 · Escritura a Delta en modo append
# ═══════════════════════════════════════════════════════════════
# Clasificación: → NARROW ✅
# Cada task escribe SU partición como uno o más archivos Parquet nuevos y
# reporta la lista al driver, que la consolida en un único commit del
# _delta_log. Los archivos que ya existían no se leen ni se comparan: en
# append solo se AGREGAN entradas "add" al log, nunca "remove".
#
# Contraste con el MERGE de 02_silver.py (Parte 3.7): allí Spark tiene
# que decidir, por cada fila fuente, si ya existe una fila destino con la
# misma clave -- eso es un join, y para que las dos filas con la misma
# clave coincidan en el mismo executor hace falta reparticionar ambos
# lados por esa clave. Ese shuffle (Exchange hashpartitioning) es lo que
# hace el MERGE WIDE ❌ y el append NARROW ✅.
(
    df_bronze.write
    .format("delta")
    .mode("append")
    .save(BRONZE)
)

print(f"Bronze escrito en: {BRONZE}")

# ═══════════════════════════════════════════════════════════════
# Verificación: los problemas del raw SÍ deben estar en Bronze
# ═══════════════════════════════════════════════════════════════
# Si Bronze estuviera limpio en este punto, algo se transformó de más
# -- eso sería un error de diseño, no una mejora.
df_check = spark.read.format("delta").load(BRONZE)

n_total = df_check.count()
n_pedido_null = df_check.filter(F.col("pedido_id").isNull()).count()
n_total_null = df_check.filter(F.col("total").isNull()).count()

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
