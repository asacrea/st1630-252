"""00_profiling.py — Lab 1b (ST1630-2026-2, S5-S6)

Exploración del dataset ANTES de limpiar nada. Corre esto primero,
copia sus salidas relevantes a `data_profiling.md` (usa
`../plantillas/data_profiling_template.md` como base) y respóndete las
8 preguntas del README antes de tocar una sola línea del pipeline
Silver.

Uso (spark-submit en el master de tu clúster EMR, o desde una celda de
EMR Studio):
    spark-submit 00_profiling.py

Qué puedes delegar aquí: nada del contenido de las respuestas -- ver
../README.md, sección "Bitácora de delegación". Sí puedes delegar
ayuda de sintaxis si algún método de PySpark no lo recuerdas.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ST1630-Lab1b-Profiling").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "32")  # clúster del curso: 4 executors x 8 cores

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-tu-usuario"  # EDITAR: el mismo bucket del Lab 1a
RAW = f"s3a://{BUCKET}/raw/ventas_colombia_raw.csv"
# Local (si corres contra una copia descargada, sin EMR):
# RAW = "../datos/ventas_colombia_raw.csv"
# ─────────────────────────────────────────────────────────────

# Todo como string -- el profiling no debe asumir tipos todavía; ver
# la misma justificación en 01_bronze.py.
df = spark.read.option("header", "true").csv(RAW)
df.cache()

n_total = df.count()
print(f"\n=== Filas totales: {n_total:,} ===")

# ── Duplicados exactos ────────────────────────────────────────
# WIDE ❌ Exchange: dropDuplicates() sobre todas las columnas necesita
# que Spark calcule el hash de la fila completa y reparticione por ese
# hash, para que dos filas idénticas -- que pueden venir de particiones
# distintas del archivo original -- terminen comparándose en el mismo
# executor. Vas a reencontrar esta misma clasificación en 02_silver.py
# (Parte 3.1) y vas a poder confirmarla en Spark UI.
n_unicas = df.dropDuplicates().count()
print(f"Duplicados exactos: {n_total - n_unicas:,} ({(n_total - n_unicas) / n_total:.2%})")

# ── Nulos por columna ──────────────────────────────────────────
print("\n=== Nulos por columna ===")
exprs = [F.sum(F.when(F.col(c).isNull() | (F.col(c) == ""), 1).otherwise(0)).alias(c) for c in df.columns]
nulos = df.select(exprs).collect()[0].asDict()
for col, cnt in sorted(nulos.items(), key=lambda kv: -kv[1]):
    print(f"  {col:<20} {cnt:>8,}  ({cnt / n_total:.2%})")

# ── Formatos de fecha ──────────────────────────────────────────
# Clasificación por regex -- no intenta parsear, solo agrupar por
# "forma". La lista de formatos reales se arma en la Parte 3.2 del lab.
print("\n=== Formatos de fecha detectados (top 10 por patrón) ===")
df_fecha = df.withColumn(
    "patron_fecha",
    F.when(F.col("fecha").rlike(r"^\d{4}-\d{2}-\d{2}$"), "yyyy-MM-dd")
     .when(F.col("fecha").rlike(r"^\d{4}/\d{2}/\d{2}$"), "yyyy/MM/dd")
     .when(F.col("fecha").rlike(r"^\d{2}-\d{2}-\d{4}$"), "dd-MM-yyyy")
     .when(F.col("fecha").rlike(r"^\d{2}/\d{2}/\d{4}$"), "dd/MM/yyyy o MM/dd/yyyy (ambiguo)")
     .otherwise("SIN RECONOCER"),
)
df_fecha.groupBy("patron_fecha").count().orderBy(F.desc("count")).show(10, truncate=False)

# ── Distribución de región (las 35 variantes deben aparecer aquí) ──
print("\n=== Valores únicos de 'region' (ordenados por frecuencia) ===")
df.groupBy("region").count().orderBy(F.desc("count")).show(40, truncate=False)
print(f"Total de valores distintos en 'region': {df.select('region').distinct().count()}")

# ── Distribución de canal ──────────────────────────────────────
print("\n=== Valores únicos de 'canal' (ordenados por frecuencia) ===")
df.groupBy("canal").count().orderBy(F.desc("count")).show(25, truncate=False)
print(f"Total de valores distintos en 'canal': {df.select('canal').distinct().count()}")

# ── Estadísticas de total / precio_unit / cantidad ─────────────
df_num = df.withColumn("total_num", F.col("total").cast("double")) \
           .withColumn("precio_num", F.col("precio_unit").cast("double")) \
           .withColumn("cantidad_num", F.col("cantidad").cast("double"))

print("\n=== Estadísticas de 'total' ===")
df_num.select(
    F.min("total_num").alias("min"),
    F.max("total_num").alias("max"),
    F.avg("total_num").alias("mean"),
    F.sum(F.when(F.col("total_num").isNull(), 1).otherwise(0)).alias("nulos"),
    F.sum(F.when(F.col("total_num") < 0, 1).otherwise(0)).alias("negativos"),
    F.sum(F.when(F.col("total_num") == 0, 1).otherwise(0)).alias("ceros"),
).show(truncate=False)

print("=== Estadísticas de 'precio_unit' ===")
df_num.select(
    F.min("precio_num").alias("min"),
    F.max("precio_num").alias("max"),
    F.sum(F.when(F.col("precio_num") < 0, 1).otherwise(0)).alias("negativos"),
).show(truncate=False)

print("=== Estadísticas de 'cantidad' ===")
df_num.select(
    F.min("cantidad_num").alias("min"),
    F.max("cantidad_num").alias("max"),
    F.sum(F.when(F.col("cantidad_num") <= 0, 1).otherwise(0)).alias("cero_o_negativo"),
).show(truncate=False)

# ── TODO: vendedor_id -- clasificación de tipos ─────────────────
# Vas a necesitar exactamente esta misma lógica en 02_silver.py
# (Parte 3.6), así que vale la pena resolverla bien aquí primero.
#
# TODO: usando F.when()/otherwise(), crea una columna "tipo_vendedor"
# que clasifique cada fila en:
#   - "entero"    si vendedor_id son solo dígitos (rlike r"^\d+$")
#   - "prefijado" si empieza con "VEN-" (startswith)
#   - "mixto"     cualquier otro caso (otherwise)
# print("\n=== Tipos detectados en 'vendedor_id' ===")
# df_vend = df.withColumn("tipo_vendedor", ...)  # TODO
# df_vend.groupBy("tipo_vendedor").count().orderBy(F.desc("count")).show(truncate=False)

# ── TODO: Validación de email ───────────────────────────────────
# TODO: define un patrón regex razonable de email (usuario@dominio.tld)
# y cuenta cuántos emails son nulos vs. cuántos tienen formato
# inválido (no nulos, pero no calzan el patrón). La misma expresión te
# sirve para la columna "email_valido" que vas a construir en
# 02_silver.py (Parte 3.6).
# print("\n=== Validación de 'email_cliente' ===")
# email_valido_pattern = r"..."  # TODO
# n_email_nulo = ...      # TODO
# n_email_invalido = ...  # TODO
# print(f"Emails nulos: {n_email_nulo:,}")
# print(f"Emails con formato inválido (no nulos): {n_email_invalido:,}")

# ── Muestras de cada tipo de problema ──────────────────────────
print("\n=== Muestra: 3 filas con pedido_id nulo ===")
df.filter(F.col("pedido_id").isNull()).show(3, truncate=False)

print("=== Muestra: 3 filas con total nulo ===")
df.filter(F.col("total").isNull() | (F.col("total") == "")).show(3, truncate=False)

print("=== Muestra: 3 filas con precio_unit negativo ===")
df_num.filter(F.col("precio_num") < 0).show(3, truncate=False)

# ── Resumen final ───────────────────────────────────────────────
print("""
=== Hallazgos que debes documentar en data_profiling.md ===
(ver la sección "El dataset -- conoce tus datos antes de
transformarlos" de ../README.md para el detalle de cada pregunta)

1. ¿Cuántos duplicados exactos tiene el dataset?
2. ¿Cuántos formatos de fecha distintos puedes identificar? Lista al
   menos 3 con ejemplos reales del dataset.
3. ¿Cuántas variantes de "Bogotá" existen? Lístalas todas.
4. ¿Cuántas variantes de "app_movil" existen? Lístalas todas.
5. ¿Qué porcentaje de filas tiene total <= 0 o nulo?
6. ¿Qué tipo de dato tiene la columna vendedor_id? ¿Es consistente?
7. ¿Qué regla de negocio permite detectar errores en 'total'?
""")

spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1
