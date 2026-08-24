"""03_gold.py — Lab 1b (ST1630-2026-2, S5-S6)

Silver -> Gold: los KPIs de negocio, ya agregados y listos para
consultar desde Athena. Todo lo que hay en Gold es, por definición, un
resumen -- nunca la granularidad de un pedido individual.

Los bloques marcados con # TODO son tu trabajo. El KPI 3 en particular
no trae ninguna implementación de referencia -- lo diseñas tú desde
cero (ver 4.3 más abajo).

Uso:
    spark-submit 03_gold.py

Qué puedes delegar: sintaxis puntual de Window/groupBy si te trabas.
Qué NO puedes delegar: el diseño del KPI 3, y la clasificación
NARROW/WIDE de cada bloque que completes.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ST1630-Lab1b-Gold").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "32")

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-nvcardozaa-2026"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
GOLD = f"s3a://{BUCKET}/gold/kpis"
# ─────────────────────────────────────────────────────────────

df_silver = spark.read.format("delta").load(SILVER)
df_silver.cache()
print(f"Filas en Silver: {df_silver.count():,}")

# ═══════════════════════════════════════════════════════════════
# TODO 4.1 · KPI 1 — Ventas por región y fecha
# ═══════════════════════════════════════════════════════════════
# TODO: agrupa df_silver por ("region", "fecha") y calcula estas 5
# métricas con .agg(...):
#   - ventas_totales        = suma de total_silver
#   - num_pedidos            = conteo de pedido_id
#   - ticket_promedio        = promedio de total_silver
#   - tasa_devolucion        = promedio de devuelto (cástalo a double primero)
#   - calificacion_promedio  = promedio de calificacion
#
# Clasificación: Es WIDE porque utiliza groupBy("region", "fecha") para agrupar las filas según dos claves; para realizar esta agrupación, Spark necesita redistribuir las filas entre los diferentes executors de acuerdo con esas claves, lo que implica un shuffle (después del shuffle se calculan las agregaciones como suma de ventas, cantidad de pedidos, promedio del ticket, tasa de devolución y calificación promedio).

kpi_ventas = df_silver.groupBy("region", "fecha").agg(
    F.sum("total_silver").alias("ventas_totales"),
    F.count("pedido_id").alias("num_pedidos"),
    F.avg("total_silver").alias("ticket_promedio"),
    F.avg(F.col("devuelto").cast("boolean").cast("double")).alias("tasa_devolucion"),
    F.avg("calificacion").alias("calificacion_promedio"),
)
print(f"4.1 KPI ventas por región/fecha: {kpi_ventas.count():,} filas")
print(f"ventas_totales: {kpi_ventas.select(F.sum('ventas_totales')).first()[0]:,.2f}")
print(f"num_pedidos: {kpi_ventas.select(F.sum('num_pedidos')).first()[0]:,}")
print(f"ticket_promedio: {kpi_ventas.select(F.avg('ticket_promedio')).first()[0]:,.2f}")
print(f"tasa_devolucion: {kpi_ventas.select(F.avg('tasa_devolucion')).first()[0]:,.4f}")
print(f"calificacion_promedio: {kpi_ventas.select(F.avg('calificacion_promedio')).first()[0]:,.2f}")

# ═══════════════════════════════════════════════════════════════
# TODO 4.2 · KPI 2 — Top 3 productos por categoría
# ═══════════════════════════════════════════════════════════════
# TODO paso 1: agrupa df_silver por ("categoria", "producto") y suma
# total_silver en una columna llamada "ventas_producto".
#
# Clasificación: Es una transformación WIDE porque se utiliza groupBy("categoria", "producto"). Spark debe redistribuir los registros para que los que pertenecen a la misma categoría y producto queden juntos. Esto genera un shuffle, necesario para calcular la suma de total_silver de cada producto.
ventas_por_producto = df_silver.groupBy("categoria", "producto").agg(
    F.sum("total_silver").alias("ventas_producto")
)
# TODO paso 2: usando pyspark.sql.window.Window, define una ventana
# particionada por "categoria" y ordenada descendentemente por
# "ventas_producto". Aplica F.rank() sobre esa ventana en una columna
# "rank", filtra rank <= 3, y descarta la columna "rank" al final.
#
# Clasificación: → También es WIDE, porque Window particiona los datos por categoria y los ordena por ventas_producto de forma descendente. Spark necesita reorganizar los datos para que los productos de una misma categoría puedan ser ordenados y posteriormente asignarles el rank. Por esta razón se requiere otro shuffle.
ventana = Window.partitionBy("categoria").orderBy(F.desc("ventas_producto"))

kpi_top_productos = (
    ventas_por_producto
    .withColumn("rank", F.rank().over(ventana))
    .filter(F.col("rank") <= 3)
    .drop("rank")
)
print(f"4.2 KPI top 3 productos por categoría: {kpi_top_productos.count():,} filas")

# ═══════════════════════════════════════════════════════════════
# TODO 4.3 (RETO) · KPI 3 — Cohortes de clientes por canal
# ═══════════════════════════════════════════════════════════════
# No hay implementación de referencia para este KPI -- lo diseñas tú.
#
# Consigna: usando "canal" y "metodo_pago" (u otra combinación de
# columnas que te parezca más interesante desde Silver), construye un
# KPI que agrupe pedidos únicos y calcule alguna métrica de calidad o
# comportamiento (p. ej. tasa de devolución, ticket promedio,
# calificación promedio) por esa combinación. Documenta en
# pipeline_analysis.md qué pregunta de negocio responde tu diseño y
# por qué elegiste esa agregación en particular.
#
# Clasificación: Es una transformación WIDE, debido al uso de groupBy("canal", "metodo_pago"). Spark debe redistribuir los registros entre las particiones para reunir aquellos que tienen la misma combinación de canal y método de pago. Esta redistribución implica un shuffle, por lo que se clasifica como WIDE.
kpi_cohortes = df_silver.groupBy("canal", "metodo_pago").agg(
    F.countDistinct("pedido_id").alias("num_pedidos"),
    F.avg("total_silver").alias("ticket_promedio"),
    F.avg(F.col("devuelto").cast("boolean").cast("double")).alias("tasa_devolucion"),
    F.avg("calificacion").alias("calificacion_promedio"),
)
print(f"4.3 KPI cohortes: {kpi_cohortes.count():,} filas")

# ═══════════════════════════════════════════════════════════════
# Escribir Gold (dado)
# ═══════════════════════════════════════════════════════════════
(
    kpi_ventas.write.format("delta").mode("overwrite")
    .option("mergeSchema", "true")
    .save(f"{GOLD}/ventas_region_fecha")
)
(
    kpi_top_productos.write.format("delta").mode("overwrite")
    .save(f"{GOLD}/top_productos_categoria")
)
(
    kpi_cohortes.write.format("delta").mode("overwrite")
    .save(f"{GOLD}/cohortes_canal_pago")
)
# ═══════════════════════════════════════════════════════════════
# TODO 4.4 · OPTIMIZE + ZORDER BY
# ═══════════════════════════════════════════════════════════════
# OPTIMIZE compacta los archivos Parquet pequeños que cada escritura
# fue dejando en archivos más grandes y eficientes de leer. ZORDER BY
# va un paso más allá: reordena físicamente las filas DENTRO de esos
# archivos para que valores similares de las columnas indicadas queden
# juntos en el mismo rango de archivos.
#
# TODO: con spark.sql(...), ejecuta un OPTIMIZE ... ZORDER BY sobre la
# tabla `{GOLD}/ventas_region_fecha`, usando las columnas por las que
# más se va a filtrar en Athena (pista: ¿qué WHERE usa la query de
# negocio de la Parte 5.1 del lab?).
#

spark.sql(f"OPTIMIZE delta.`{GOLD}/ventas_region_fecha` ZORDER BY (region, fecha)")
print("4.4 OPTIMIZE + ZORDER BY aplicado sobre ventas_region_fecha")

# ═══════════════════════════════════════════════════════════════
# 4.5 · Registrar en Glue Catalog (un ejemplo dado + 2 por tu cuenta)
# ═══════════════════════════════════════════════════════════════
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_ventas_region_fecha
    USING DELTA
    LOCATION '{GOLD}/ventas_region_fecha'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_top_productos_categoria
    USING DELTA
    LOCATION '{GOLD}/top_productos_categoria'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_cohortes_canal_pago
    USING DELTA
    LOCATION '{GOLD}/cohortes_canal_pago'
""")

print("4.5 Tablas registradas en Glue Catalog -- listas para consultar desde Athena")

spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1
