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
BUCKET = "st1630-jjdiazr-2026"  # EDITAR: el mismo bucket del Lab 1a
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
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica: ¿por
# qué un groupBy + agg necesita mover filas entre executors?
# TU RESPUESTA: WIDE ❌ -- las filas de una misma combinación región-fecha
# están repartidas por varias particiones, y no se puede sumar un grupo
# sin tenerlo completo. Spark redistribuye con
# hashpartitioning(region, fecha, 32) para que todas las filas de
# ("BOGOTÁ", 2026-03-03) queden juntas antes de agregar.
#
# En el plan hay una agregación parcial ANTES del Exchange: cada executor
# pre-suma lo que ya tiene y solo manda esos parciales por red, en vez de
# las filas crudas. Eso reduce el volumen del shuffle, pero no lo elimina.
# OJO: 'devuelto' y 'calificacion' siguen siendo STRING en Silver
# (vienen de Bronze, que es 100% string, y la selección final de
# 02_silver.py no los castea). Un .cast("double") directo sobre el
# string "True" da NULL, no 1.0 -- por eso 'devuelto' pasa primero por
# boolean. Sin esto, tasa_devolucion saldría toda en null.
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
# Clasificación: → [tu respuesta] -- justifica.
# TU RESPUESTA: WIDE ❌ -- mismo razonamiento que 4.1 con otra clave: las
# ventas de un mismo producto están repartidas en varias particiones y hay
# que juntarlas para sumarlas. Clave del shuffle:
# hashpartitioning(categoria, producto, 32).
ventas_por_producto = df_silver.groupBy("categoria", "producto").agg(
    F.sum("total_silver").alias("ventas_producto")
)

# TODO paso 2: usando pyspark.sql.window.Window, define una ventana
# particionada por "categoria" y ordenada descendentemente por
# "ventas_producto". Aplica F.rank() sobre esa ventana en una columna
# "rank", filtra rank <= 3, y descarta la columna "rank" al final.
#
# Clasificación: → [tu respuesta] -- justifica (pista: ¿por qué esta
# Window necesita OTRO shuffle además del que ya hizo el groupBy del
# paso 1, si la clave de partición es distinta?).
# TU RESPUESTA: WIDE ❌, y necesita un Exchange PROPIO porque el
# particionamiento del paso 1 no le sirve. Con hash(categoria, producto),
# "Audífonos" y "Teclado" -- ambos de Electrónica -- caen en particiones
# distintas, porque el hash se calculó sobre la PAREJA y no sobre la
# categoría sola. Para rankear los 3 mejores productos de Electrónica hay
# que tener todos sus productos juntos, así que Spark vuelve a
# redistribuir con hashpartitioning(categoria, 32).
#
# La lección: que dos pasos "agrupen por categoría" no significa que
# compartan particionamiento. Lo que determina dónde cae una fila es la
# clave COMPLETA del hash, y hash(a, b) != hash(a).
#
# Confirmado en el plan físico del KPI 2, que tiene dos Exchange:
#     (4) hashpartitioning(categoria, producto, 32)   <- groupBy
#     (6) hashpartitioning(categoria, 32)             <- Window
ventana_categoria = Window.partitionBy("categoria").orderBy(F.desc("ventas_producto"))
kpi_top_productos = (
    ventas_por_producto
    .withColumn("rank", F.rank().over(ventana_categoria))
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
# Clasificación: → [tu respuesta] -- cualquier groupBy/agg que uses
# aquí, justifica por qué es NARROW o WIDE.
#
# TU RESPUESTA: WIDE ❌ -- el groupBy("categoria","canal") es el mismo caso
# que 4.1 y 4.2: los pedidos de una cohorte categoría-canal están
# repartidos en varias particiones y hay que juntarlos para promediar.
# Clave: hashpartitioning(categoria, canal, 32). Un solo Exchange, porque
# las cuatro métricas se calculan en la misma agregación.
#
# Los cast() de devuelto y calificacion son NARROW: se resuelven fila a
# fila antes del shuffle.
# TU DISEÑO:
# Pregunta de negocio: ¿hay categorías de producto que decepcionan
# según el canal por el que se compran? La hipótesis es que los
# productos que uno querría ver o probar antes de comprar (ropa,
# deportes) califican peor cuando se compran a ciegas por app o web
# que cuando se compran en tienda física.
#
# Dimensiones: categoria x canal  -> 5 x 4 = 20 cohortes
# Métrica principal: calificacion_promedio (satisfacción, 1-5)
# Métricas de apoyo: num_pedidos (para saber si el promedio es
#   confiable o son 3 pedidos), y tasa_devolucion (una calificación
#   baja debería venir acompañada de más devoluciones -- si las dos
#   señales coinciden, la conclusión es más sólida).
kpi_cohortes = df_silver.groupBy("categoria", "canal").agg(
    F.avg(F.col("calificacion").cast("double")).alias("calificacion_promedio"),
    F.count("pedido_id").alias("num_pedidos"),
    F.avg(F.col("devuelto").cast("boolean").cast("double")).alias("tasa_devolucion"),
    F.avg("total_silver").alias("ticket_promedio"),
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
# TU RESPUESTA -- revisa el orden de las columnas antes de correr:
# la query de 5.1 filtra por rango de FECHA (últimos 3 meses) y agrupa
# por región, así que 'fecha' va primero por ser la del WHERE. Si
# decides otro orden, justifícalo en pipeline_analysis.md.
spark.sql(f"OPTIMIZE delta.`{GOLD}/ventas_region_fecha` ZORDER BY (fecha, region)")

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
# TU RESPUESTA:
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
