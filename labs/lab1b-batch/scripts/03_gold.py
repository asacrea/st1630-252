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
spark.conf.set("spark.sql.shuffle.partitions", "32")  # clúster del curso: 4 executors x 8 cores

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-samedinac-2023"  # EDITAR: el mismo bucket del Lab 1a
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
# Clasificación: → WIDE ❌ -- groupBy reune todas las filas de cada
# (region, fecha) en el mismo executor; Spark hashea esas claves y
# hace shuffle write/read (Exchange) para agregar.
kpi_ventas = df_silver.groupBy("region", "fecha").agg(
    F.sum("total_silver").alias("ventas_totales"),
    F.count("pedido_id").alias("num_pedidos"),
    F.avg("total_silver").alias("ticket_promedio"),
    F.avg(F.col("devuelto").cast("boolean").cast("double")).alias("tasa_devolucion"),
    F.avg(F.col("calificacion").cast("double")).alias("calificacion_promedio"),
)
print(f"4.1 KPI ventas por región/fecha: {kpi_ventas.count():,} filas")

# ═══════════════════════════════════════════════════════════════
# TODO 4.2 · KPI 2 — Top 3 productos por categoría
# ═══════════════════════════════════════════════════════════════
# TODO paso 1: agrupa df_silver por ("categoria", "producto") y suma
# total_silver en una columna llamada "ventas_producto".
#
# Clasificación: → WIDE ❌ -- groupBy (categoria, producto) shufflea
# por el hash de ambas claves para sumar ventas_producto.
ventas_por_producto = df_silver.groupBy("categoria", "producto").agg(
    F.sum("total_silver").alias("ventas_producto"),
)

# TODO paso 2: usando pyspark.sql.window.Window, define una ventana
# particionada por "categoria" y ordenada descendentemente por
# "ventas_producto". Aplica F.rank() sobre esa ventana en una columna
# "rank", filtra rank <= 3, y descarta la columna "rank" al final.
#
# Clasificación: → WIDE ❌ -- rank() sobre partitionBy("categoria")
# necesita todas las filas de una categoría juntas. El groupBy anterior
# particionó por (categoria, producto), no solo por categoria, así que
# Spark vuelve a shufflear.
w_top = Window.partitionBy("categoria").orderBy(F.desc("ventas_producto"))
kpi_top_productos = (
    ventas_por_producto
    .withColumn("rank", F.rank().over(w_top))
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
# Clasificación: → WIDE ❌ -- groupBy (canal, metodo_pago) shufflea
# para que todos los pedidos de esa cohorte se agreguen juntos.
#
# Pregunta de negocio: ¿qué par canal × método de pago concentra más
# ventas, con qué ticket, tasa de devolución y calidad de email?
# Sirve para priorizar fraude/logística (p. ej. telefono+efectivo vs
# app_movil+tarjeta) sin bajar a granularidad de pedido.
kpi_cohortes = df_silver.groupBy("canal", "metodo_pago").agg(
    F.count("pedido_id").alias("num_pedidos"),
    F.sum("total_silver").alias("ventas_totales"),
    F.avg("total_silver").alias("ticket_promedio"),
    F.avg(F.col("devuelto").cast("boolean").cast("double")).alias("tasa_devolucion"),
    F.avg(F.col("calificacion").cast("double")).alias("calificacion_promedio"),
    F.avg(F.col("email_valido").cast("double")).alias("tasa_email_valido"),
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
# (tu código aquí)
spark.sql(f"""
    OPTIMIZE delta.`{GOLD}/ventas_region_fecha`
    ZORDER BY (fecha, region)
""")

print("4.4 OPTIMIZE + ZORDER BY aplicado sobre ventas_region_fecha")

# ═══════════════════════════════════════════════════════════════
# 4.5 · Registrar en Glue Catalog (un ejemplo dado + 2 por tu cuenta)
# ═══════════════════════════════════════════════════════════════
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_ventas_region_fecha
    USING DELTA
    LOCATION '{GOLD}/ventas_region_fecha'
""")

# TODO: registra las otras dos tablas Gold en Glue Catalog con el
# mismo patrón que el ejemplo de arriba, con nombres
# "gold_top_productos_categoria" y "gold_cohortes_canal_pago".
# (tu código aquí)
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
