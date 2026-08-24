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

# SparkSession es el punto de entrada a la ejecución distribuida. Este
# script no habilita Delta porque solo lee el CSV crudo y genera
# estadísticas; todavía no escribe ninguna tabla medallion.
spark = SparkSession.builder.appName("ST1630-Lab1b-Profiling").getOrCreate()

# Los groupBy, distinct y agregados del profiling generan shuffles. Se
# usan 32 particiones porque el clúster del curso tiene 4 executors x 8
# cores: hay paralelismo suficiente sin crear 200 tareas diminutas para
# un dataset de apenas 101.500 filas.
spark.conf.set("spark.sql.shuffle.partitions", "32")

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-ssalazarh3-2026"  # EDITAR: el mismo bucket del Lab 1a
RAW = f"s3a://{BUCKET}/raw/ventas_colombia_raw.csv"
# Local (si corres contra una copia descargada, sin EMR):
# RAW = "../datos/ventas_colombia_raw.csv"
# ─────────────────────────────────────────────────────────────

# Todo se lee como string: el profiling debe observar la representación
# original antes de decidir qué valores son válidos o cómo convertirlos.
# Permitir inferSchema aquí podría ocultar problemas al transformar
# automáticamente celdas vacías o valores incompatibles en null.
#
# La lectura es NARROW: cada partición puede interpretar sus propias
# líneas del CSV sin comparar filas ni intercambiar datos con otras.
df = spark.read.option("header", "true").csv(RAW)

# El DataFrame se reutiliza en muchas acciones (`count`, `groupBy`,
# muestras). cache() evita volver a descargar y parsear el mismo CSV de
# S3 para cada una. La primera acción, `count()`, materializa el caché.
df.cache()

n_total = df.count()
print(f"\n=== Filas totales: {n_total:,} ===")

# ── Duplicados exactos ────────────────────────────────────────
# WIDE Exchange: dropDuplicates() sobre todas las columnas necesita
# que Spark calcule el hash de la fila completa y reparticione por ese
# hash, para que dos filas idénticas -- que pueden venir de particiones
# distintas del archivo original -- terminen comparándose en el mismo
# executor. Vas a reencontrar esta misma clasificación en 02_silver.py
# (Parte 3.1) y vas a poder confirmarla en Spark UI.
n_unicas = df.dropDuplicates().count()
print(f"Duplicados exactos: {n_total - n_unicas:,} ({(n_total - n_unicas) / n_total:.2%})")

# ── Nulos por columna ──────────────────────────────────────────
# Las expresiones when se evalúan fila a fila (NARROW), pero collect()
# necesita combinar las sumas parciales de todas las particiones en un
# único resultado global; esa reducción final introduce un Exchange.
print("\n=== Nulos por columna ===")
exprs = [
    F.sum(
        F.when(F.col(c).isNull() | (F.col(c) == ""), 1).otherwise(0)
    ).alias(c)
    for c in df.columns
]
nulos = df.select(exprs).collect()[0].asDict()
for col, cnt in sorted(nulos.items(), key=lambda kv: -kv[1]):
    print(f"  {col:<20} {cnt:>8,}  ({cnt / n_total:.2%})")

# ── Formatos de fecha ──────────────────────────────────────────
# La clasificación por regex es NARROW: cada fecha se compara con
# patrones usando únicamente el contenido de su fila. No se parsea aún
# porque el objetivo es descubrir la variedad del dato, no limpiarlo.
#
# El groupBy + count posterior es WIDE: Spark redistribuye las filas
# por `patron_fecha` para reunir todos los registros del mismo formato.
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
# WIDE: groupBy necesita un shuffle por el valor crudo de `region`.
# El orderBy puede añadir ordenamiento global. Esto permite detectar
# diferencias invisibles a simple vista, como mayúsculas o espacios.
print("\n=== Valores únicos de 'region' (ordenados por frecuencia) ===")
df.groupBy("region").count().orderBy(F.desc("count")).show(40, truncate=False)
print(f"Total de valores distintos en 'region': {df.select('region').distinct().count()}")

# ── Distribución de canal ──────────────────────────────────────
# También es WIDE por el groupBy. El inventario resultante será la
# evidencia para construir MAPA_CANAL en Silver.
print("\n=== Valores únicos de 'canal' (ordenados por frecuencia) ===")
df.groupBy("canal").count().orderBy(F.desc("count")).show(25, truncate=False)
print(f"Total de valores distintos en 'canal': {df.select('canal').distinct().count()}")

# ── Estadísticas de total / precio_unit / cantidad ─────────────
# Los cast son NARROW ✅: convierten cada celda de forma independiente.
# Un texto no convertible queda como null, lo cual permite contarlo sin
# detener el profiling. Los min/max/avg/sum posteriores son agregados
# globales y, por tanto, requieren combinar resultados parciales.
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

# ── vendedor_id: clasificación de representaciones ─────────────
# El objetivo no es decidir todavía el formato definitivo, sino medir
# cuántas convenciones distintas usa el origen. El withColumn es
# NARROW; el groupBy es WIDE porque reúne las tres clases.
print("\n=== Tipos detectados en 'vendedor_id' ===")
df_vend = df.withColumn(
    "tipo_vendedor",
    F.when(F.col("vendedor_id").rlike(r"^\d+$"), "entero")
    .when(F.col("vendedor_id").startswith("VEN-"), "prefijado")
    .otherwise("mixto"),
)
df_vend.groupBy("tipo_vendedor").count().orderBy(
    F.desc("count")
).show(truncate=False)

# ── Validación exploratoria de email ───────────────────────────
# Este regex comprueba la estructura mínima usuario@dominio.tld. No
# pretende implementar todo el estándar RFC 5322: para calidad de datos
# es preferible una regla entendible que identifique los errores
# evidentes del dataset.
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# Las condiciones se calculan fila a fila, pero agg combina los
# conteos parciales y produce un resultado global (Exchange).
print("\n=== Validación de 'email_cliente' ===")
email_stats = df.select(
    F.sum(
        F.when(
            F.col("email_cliente").isNull()
            | (F.col("email_cliente") == ""),
            1,
        ).otherwise(0)
    ).alias("nulos"),
    F.sum(
        F.when(
            F.col("email_cliente").isNotNull()
            & (F.col("email_cliente") != "")
            & ~F.col("email_cliente").rlike(EMAIL_PATTERN),
            1,
        ).otherwise(0)
    ).alias("invalidos_no_nulos"),
).collect()[0]

print(f"Emails nulos: {email_stats['nulos']:,}")
print(
    "Emails con formato inválido (no nulos): "
    f"{email_stats['invalidos_no_nulos']:,}"
)

# ── Muestras de cada tipo de problema ──────────────────────────
# filter es NARROW y show es una acción. Estas muestras sirven como
# evidencia cualitativa: los conteos dicen cuánto ocurre y las filas
# permiten verificar cómo se ve realmente cada problema.
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

# Libera los bloques almacenados antes de cerrar la aplicación. Aunque
# spark.stop() también termina los recursos, explicitarlo documenta que
# el caché solo era necesario durante este profiling.
df.unpersist()
spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1