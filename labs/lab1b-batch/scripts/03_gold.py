"""03_gold.py — Lab 1b (ST1630-2026-2, S5-S6)

Silver -> Gold: construye KPIs agregados de negocio y los deja listos
para consultarlos desde Athena. Gold contiene resúmenes; no conserva la
granularidad de un pedido individual.

Uso:
    spark-submit 03_gold.py

Si el clúster no fue creado con Delta habilitado, carga los JAR que EMR
6.15 instala localmente:
    spark-submit --jars /usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar 03_gold.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("ST1630-Lab1b-Gold")
    # Hive support permite que los CREATE TABLE queden registrados en
    # el catálogo configurado por EMR (Glue en el laboratorio).
    .enableHiveSupport()
    .getOrCreate()
)

# Los groupBy y las ventanas de Gold son WIDE. Se usan 32 particiones
# para mantener ocupados los 4 executors x 8 cores sin el overhead de
# las 200 particiones predeterminadas.
spark.conf.set("spark.sql.shuffle.partitions", "32")

# ─────────────────────────────────────────────────────────────
# Rutas del laboratorio
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-ssalazarh3-2026"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
GOLD = f"s3a://{BUCKET}/gold/kpis"
CSV_10K = f"s3a://{BUCKET}/benchmark/csv_10k/"
# ─────────────────────────────────────────────────────────────

# Silver ya contiene tipos confiables, categorías normalizadas y una
# fila por pedido; por eso es la fuente correcta para los KPIs.
df_silver = spark.read.format("delta").load(SILVER)

# El mismo DataFrame alimenta tres KPIs y la muestra CSV. cache() evita
# releer Silver desde S3 para cada acción. count() materializa el caché.
df_silver.cache()

print(f"Filas en Silver: {df_silver.count():,}")

# ═══════════════════════════════════════════════════════════════
# 4.1 · KPI 1 — Ventas por región y fecha
# ═══════════════════════════════════════════════════════════════
# Pregunta de negocio:
# ¿Cuánto se vende diariamente en cada región, cuántos pedidos se
# reciben y cuál es su calidad promedio?
#
# Clasificación: WIDE. `groupBy` debe reunir en una misma partición
# todas las filas que tengan la misma región y fecha. Esta
# redistribución de datos entre executors produce un shuffle.
kpi_ventas = (
    df_silver
    .groupBy("region", "fecha")
    .agg(
        F.sum("total_silver").alias("ventas_totales"),
        F.count("pedido_id").alias("num_pedidos"),
        F.avg("total_silver").alias("ticket_promedio"),
        # Al convertir boolean a double, false=0 y true=1. El promedio
        # de esa columna es directamente la proporción de devoluciones.
        F.avg(F.col("devuelto").cast("double")).alias("tasa_devolucion"),
        F.avg("calificacion").alias("calificacion_promedio"),
    )
)

print(f"4.1 KPI ventas por región/fecha: {kpi_ventas.count():,} filas")

# ═══════════════════════════════════════════════════════════════
# 4.2 · KPI 2 — Top 3 productos por categoría
# ═══════════════════════════════════════════════════════════════
# Paso 1: calcular las ventas acumuladas por producto.
#
# Clasificación: WIDE. Spark debe reunir todos los pedidos del
# mismo producto y categoría antes de sumar sus ventas. El `groupBy`
# necesita un shuffle entre executors.
ventas_por_producto = (
    df_silver
    .groupBy("categoria", "producto")
    .agg(
        F.sum("total_silver").alias("ventas_producto")
    )
)

# Paso 2: ordenar los productos dentro de cada categoría y conservar
# los tres primeros puestos.
#
# Clasificación: WIDE. La ventana necesita volver a distribuir los
# resultados por `categoria` y ordenar cada partición por
# `ventas_producto`. Como la clave de partición ya no es exactamente
# `(categoria, producto)`, se requiere otro Exchange/shuffle.
ventana_categoria = (
    Window
    .partitionBy("categoria")
    .orderBy(F.desc("ventas_producto"))
)

# rank() conserva empates: si dos productos empatan en ventas reciben
# el mismo puesto. Por ello una categoría puede producir más de tres
# filas si existe un empate dentro de los tres primeros lugares.
kpi_top_productos = (
    ventas_por_producto
    .withColumn("rank", F.rank().over(ventana_categoria))
    .filter(F.col("rank") <= 3)
    .drop("rank")
)

print(
    "4.2 KPI top 3 productos por categoría: "
    f"{kpi_top_productos.count():,} filas"
)

# ═══════════════════════════════════════════════════════════════
# 4.3 · KPI 3 — Cohortes por canal y método de pago
# ═══════════════════════════════════════════════════════════════
# Pregunta de negocio:
# ¿Qué combinaciones de canal y método de pago generan más pedidos e
# ingresos, y cuáles presentan mejor ticket, satisfacción y tasa de
# devolución?
#
# Se cuentan pedidos distintos para proteger el KPI frente a posibles
# repeticiones de la clave. Aunque Silver debería ser única por
# pedido_id, countDistinct documenta esa regla de negocio y evita que
# una duplicación accidental infle el indicador. Además, se calculan
# métricas comerciales y de calidad para comparar cada cohorte.
#
# Clasificación: WIDE. El `groupBy` debe reunir todas las filas con
# la misma combinación `(canal, metodo_pago)` antes de calcular las
# agregaciones. Esto requiere redistribuir datos entre executors.
kpi_cohortes = (
    df_silver
    .groupBy("canal", "metodo_pago")
    .agg(
        F.countDistinct("pedido_id").alias("pedidos_unicos"),
        F.sum("total_silver").alias("ventas_totales"),
        F.avg("total_silver").alias("ticket_promedio"),
        F.avg(F.col("devuelto").cast("double")).alias("tasa_devolucion"),
        F.avg("calificacion").alias("calificacion_promedio"),
    )
)

print(f"4.3 KPI cohortes: {kpi_cohortes.count():,} filas")

# ═══════════════════════════════════════════════════════════════
# Escritura de las tablas Gold
# ═══════════════════════════════════════════════════════════════
# Gold es una capa derivada y reproducible, por eso `overwrite` es
# apropiado: cada corrida reemplaza el resumen anterior con KPIs
# consistentes con la versión actual de Silver. Esto contrasta con
# Bronze, donde se usa append para conservar la historia del origen.
(
    kpi_ventas.write
    .format("delta")
    .mode("overwrite")
    # Permite una evolución controlada si se agrega una métrica nueva.
    .option("mergeSchema", "true")
    .save(f"{GOLD}/ventas_region_fecha")
)

(
    kpi_top_productos.write
    .format("delta")
    .mode("overwrite")
    .save(f"{GOLD}/top_productos_categoria")
)

(
    kpi_cohortes.write
    .format("delta")
    .mode("overwrite")
    .save(f"{GOLD}/cohortes_canal_pago")
)

# ═══════════════════════════════════════════════════════════════
# 4.4 · OPTIMIZE + ZORDER BY
# ═══════════════════════════════════════════════════════════════
# `region` y `fecha` son las columnas utilizadas con mayor frecuencia
# para filtrar las ventas. OPTIMIZE compacta archivos pequeños y
# ZORDER aproxima físicamente los valores semejantes para favorecer el
# data skipping.
#
# Esta operación es WIDE porque reescribe y reorganiza físicamente
# datos procedentes de varios archivos. OPTIMIZE reduce el problema de
# archivos pequeños; ZORDER mejora el data skipping, pero no crea una
# partición de carpetas ni garantiza un orden global estricto.
spark.sql(
    f"""
    OPTIMIZE delta.`{GOLD}/ventas_region_fecha`
    ZORDER BY (region, fecha)
    """
)

print("4.4 OPTIMIZE + ZORDER BY aplicado sobre ventas_region_fecha")

# ═══════════════════════════════════════════════════════════════
# 4.5 · Registro de las tablas en Glue Catalog
# ═══════════════════════════════════════════════════════════════
# Estos comandos no copian los datos. Registran nombres y ubicaciones
# para que Athena pueda resolver cada tabla a su ruta Delta en S3.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS gold_ventas_region_fecha
    USING DELTA
    LOCATION '{GOLD}/ventas_region_fecha'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS gold_top_productos_categoria
    USING DELTA
    LOCATION '{GOLD}/top_productos_categoria'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS gold_cohortes_canal_pago
    USING DELTA
    LOCATION '{GOLD}/cohortes_canal_pago'
    """
)

print(
    "4.5 Tablas registradas en Glue Catalog -- "
    "listas para consultar desde Athena"
)

# ═══════════════════════════════════════════════════════════════
# Preparación del benchmark CSV de Athena
# ═══════════════════════════════════════════════════════════════
# Athena comparará Gold/Parquet contra una muestra CSV sin particionar.
# Se seleccionan exactamente las nueve columnas declaradas después en
# el DDL de `benchmark_csv_10k`; exportar columnas adicionales movería
# las posiciones y produciría lecturas incorrectas.
#
# limit(10_000) crea una muestra operativa, no una muestra estadística
# reproducible. Es suficiente aquí porque el objetivo es comparar el
# costo de lectura de formatos. coalesce(1) concentra la muestra en un
# solo archivo: para 10.000 filas evita múltiples headers y simplifica
# el benchmark, aunque no sería escalable para un volumen grande.
muestra_csv_10k = (
    df_silver
    .select(
        "pedido_id",
        "fecha",
        "region",
        "canal",
        "categoria",
        "producto",
        "cantidad",
        "precio_unit",
        "total_silver",
    )
    .limit(10_000)
    .coalesce(1)
)

# `header=true` es obligatorio porque el DDL de Athena usa
# skip.header.line.count=1. El overwrite hace que cada ejecución deje
# una sola muestra vigente y no mezcle archivos de corridas anteriores.
(
    muestra_csv_10k.write
    .mode("overwrite")
    .option("header", "true")
    .csv(CSV_10K)
)

print(f"Muestra CSV de 10.000 filas exportada en: {CSV_10K}")

# Libera explícitamente el caché cuando todos sus consumidores ya
# terminaron, antes de cerrar la aplicación Spark.
df_silver.unpersist()
spark.stop()

# Cuando termines, no olvides apagar el clúster EMR si ya no lo vas a
# utilizar en las próximas horas:
# aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1