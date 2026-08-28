"""01_bronze.py — Lab 1b (ST1630-2026-2, S5-S6)

Ingesta cruda a Bronze: el CSV entra a Delta Lake TAL CUAL viene, sin
limpiar ni tipar nada. Bronze es la capa de "verdad del origen" -- si
algo sale mal en Silver o Gold, siempre puedes volver a Bronze y
reprocesar, porque Bronze nunca se sobreescribe con datos transformados.

Esta versión está completada y comentada. Conserva el dato crudo,
añade trazabilidad técnica y verifica que Bronze no haya eliminado los
problemas que Silver debe resolver posteriormente.

Uso en un clúster creado con Delta habilitado:
    spark-submit 01_bronze.py

Si el clúster de Lab 1a se creó solo con Spark/Hadoop, carga los JAR
Delta que EMR 6.15 ya instala localmente:
    spark-submit --jars /usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar 01_bronze.py

La decisión central que debes poder explicar es por qué el schema de
Bronze es 100% string: ingerir no es lo mismo que interpretar o limpiar.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("ST1630-Lab1b-Bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
# Este valor no afecta la lectura simple, pero sí las verificaciones
# WIDE del final, como dropDuplicates(). Se ajusta al paralelismo del
# clúster del curso: 4 executors x 8 cores.
spark.conf.set("spark.sql.shuffle.partitions", "32")

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-ssalazarh3-2026"  # EDITAR: el mismo bucket del Lab 1a
RAW = f"s3a://{BUCKET}/raw/ventas_colombia_raw.csv"
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
BRONZE_CLI = f"s3://{BUCKET}/bronze/pedidos"
# ─────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════
# 1 · Schema explícito -- TODOS los campos como StringType
# ═══════════════════════════════════════════════════════════════
# Justificación: Bronze recibe el dato tal cual el sistema de
# origen lo entregó, sin asumir ningún tipo. Si aquí ya castearas
# 'total' a double, por ejemplo, Spark tendría que decidir qué hacer
# con un valor corrupto como una celda vacía o con letras -- y esa
# decisión (¿null? ¿0? ¿falla el job?) es una transformación, no una
# ingesta. La conversión de tipos es responsabilidad exclusiva de
# Silver, donde SÍ hay contexto de negocio para decidir cómo tratar
# cada caso raro.
#
# Las columnas del CSV crudo (orden físico que escribe
# ../datos/gen_dataset.py y que Spark debe respetar al aplicar el schema):
#   pedido_id, fecha, categoria, producto, cantidad, precio_unit, total,
#   email_cliente, metodo_pago, devuelto, calificacion, region, canal,
#   vendedor_id
# `True` permite null porque Bronze debe aceptar incluso registros
# incompletos. El orden importa: con un schema explícito, el lector CSV
# asigna cada campo por su posición física, no por una inferencia.

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
# 2 · Lectura del CSV con schema explícito
# ═══════════════════════════════════════════════════════════════
# Clasificación: NARROW. Cada executor puede leer sus bloques del CSV
# y aplicar el schema por posición sin comparar filas con otros
# executors. No hay repartición ni Exchange.
df_raw = (
    spark.read
    .option("header", "true")
    .schema(BRONZE_SCHEMA)
    .csv(RAW)
)

# ═══════════════════════════════════════════════════════════════
# 3 · Columnas de auditoría
# ═══════════════════════════════════════════════════════════════
# `_ingested_at` permite reconstruir cuándo entró un lote y
# `_source_file` permite rastrear la fila hasta su archivo de origen.
# Son metadatos técnicos; no alteran el contenido de negocio.
#
# Clasificación: NARROW. Ambas columnas se calculan usando el contexto
# de la propia fila/partición y no requieren mover datos por la red.
df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# ═══════════════════════════════════════════════════════════════
# 4 · Escritura a Delta en modo append
# ═══════════════════════════════════════════════════════════════
# El modo append representa la naturaleza histórica de Bronze: cada
# ingesta agrega un nuevo lote y no destruye evidencia previa. Por eso
# volver a ejecutar el script vuelve a anexar el mismo archivo; la
# deduplicación pertenece a Silver.
#
# Clasificación: NARROW respecto a la distribución. Cada partición de
# entrada escribe sus propios archivos Parquet y Delta registra los
# nuevos `add` en una transacción. No hay que comparar cada fila contra
# la tabla existente. En cambio, el MERGE de Silver es WIDE porque sí
# debe localizar por `pedido_id` qué filas actualizar o insertar.
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
# bloques anteriores quedaron bien).
# ═══════════════════════════════════════════════════════════════
# Si Bronze estuviera limpio en este punto, algo se transformó de más
# -- eso sería un error de diseño, no una mejora.
df_check = spark.read.format("delta").load(BRONZE)

n_total = df_check.count()
n_pedido_null = df_check.filter(F.col("pedido_id").isNull()).count()
n_total_null = df_check.filter(F.col("total").isNull()).count()

# WIDE Exchange: dropDuplicates() sobre TODAS las columnas necesita
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
    "Bronze no debería estar limpio -- revisa la lectura, probablemente "
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
aws s3 cp {BRONZE_CLI}/_delta_log/00000000000000000000.json - | python3 -m json.tool | head -50

Busca estas líneas dentro del JSON y anota en tu bitácora qué
representa cada una:
  - "commitInfo"  -> ¿quién hizo el commit? ¿cuándo? ¿con qué operación?
  - "metaData"    -> ¿coincide el schema con tu BRONZE_SCHEMA + las 2
                     columnas de auditoría agregadas en el paso 3?
  - "add"         -> ¿cuántos archivos Parquet agregó este commit?
""")

spark.stop()

# ### Cuandotermines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1