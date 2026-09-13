"""02_silver.py — Lab 1b (ST1630-2026-2, S5-S6)

Bronze -> Silver: limpieza, normalización y la primera escritura ACID
de verdad del lab (el MERGE). Este es el script central del laboratorio
-- cada paso está numerado igual que la Parte 3 de ../README.md.

Los bloques marcados con # TODO son tu trabajo. El resto (imports,
rutas, el helper construir_mapa(), la selección final de columnas, la
verificación con time travel) ya está resuelto -- concéntrate en los
TODO, que son justo las decisiones y el código que esta semana busca
que aprendas a escribir de memoria.

IMPORTANTE -- contratos de nombres de columna: cada TODO especifica
el nombre EXACTO de columna que debe producir. El código dado más
abajo (la selección final `df_silver = df_tipos.select(...)`) asume
esos nombres tal cual -- si los cambias, tendrás que ajustar también
esa parte.

Uso:
    spark-submit 02_silver.py

Qué puedes delegar: sintaxis puntual (¿cómo se llama la función de
regex en PySpark?). Qué NO puedes delegar: el contenido de
MAPA_REGION/MAPA_CANAL (sale de TU profiling, no del de nadie más), la
estrategia de validación de 'total', y la clasificación NARROW/WIDE de
cada bloque que completes -- ver ../README.md, "Bitácora de delegación".
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("ST1630-Lab1b-Silver").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "32")  # clúster del curso: 4 executors x 8 cores

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-dagutierrl-2026"  # EDITAR: el mismo bucket del Lab 1a
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
# ─────────────────────────────────────────────────────────────

df_bronze = spark.read.format("delta").load(BRONZE)
n_bronze = df_bronze.count()
print(f"Filas en Bronze: {n_bronze:,}")

# ═══════════════════════════════════════════════════════════════
# 3.1 · Deduplicación (dado)
# ═══════════════════════════════════════════════════════════════
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica en
# 1-2 líneas: ¿decidir si dos filas son idénticas requiere que ambas
# terminen en el mismo executor para compararse? ¿Qué te dice eso
# sobre si hay un shuffle físico detrás de esta llamada, aunque no
# haya ningún groupBy ni join explícito en el código?
df_dedup = df_bronze.dropDuplicates()
n_dedup = df_dedup.count()
print(f"3.1 Deduplicación: {n_bronze:,} -> {n_dedup:,} filas (-{n_bronze - n_dedup:,} duplicados)")

# ═══════════════════════════════════════════════════════════════
# TODO 3.2 · Fechas -- el reto de los 5 formatos
# ═══════════════════════════════════════════════════════════════
# En tu data_profiling.md (Pregunta 2) ya identificaste los 5 formatos
# de fecha del dataset. Vas a necesitar el nombre de patrón de Spark
# para cada uno -- revisa la documentación de `to_date()` si no
# recuerdas la sintaxis de los patrones (p. ej. "yyyy-MM-dd").
#
# TODO: define FORMATOS_FECHA como una lista de los 5 patrones de
# fecha, en el ORDEN en que quieres que Spark los intente (piensa en
# qué pasa si dos formatos son ambiguos entre sí -- ¿cuál debería ir
# primero?).
FORMATOS_FECHA = [
    "yyyy/MM/dd",
    "yyyy-MM-dd",
    "dd-MM-yyyy",
    "dd/MM/yyyy",
] # TODO: completa con los 5 patrones, en el orden que decidas

# TODO: usa F.coalesce(...) combinando un F.to_date(F.col("fecha"), fmt)
# por cada formato de FORMATOS_FECHA, y guarda el resultado en una
# columna nueva llamada EXACTAMENTE "fecha_parsed" (withColumn).
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica.

df_fechas = df_dedup.withColumn(
    "fecha_parsed",
    F.coalesce(
        *[
            F.to_date(F.col("fecha"), formato)
            for formato in FORMATOS_FECHA
        ]
    )
)  # TODO: reemplaza por df_dedup + la columna "fecha_parsed"

n_sin_fecha = df_fechas.filter(F.col("fecha_parsed").isNull()).count()
print(f"3.2 Fechas: {n_sin_fecha:,} filas sin ningún formato reconocido (se descartan)")
df_fechas = df_fechas.filter(F.col("fecha_parsed").isNotNull())

# Nota: 'dd/MM/yyyy' y 'MM/dd/yyyy' son ambiguos para días <= 12 -- el
# orden de tu lista decide cuál gana, no hay forma de saberlo con
# certeza solo con el dato. Si te interesa, coméntalo en
# pipeline_analysis.md (no es una de las 5 preguntas obligatorias, pero
# demuestra que entendiste la limitación).

# ═══════════════════════════════════════════════════════════════
# TODO 3.3 · Normalización de región -- el reto principal
# ═══════════════════════════════════════════════════════════════
# Este es el ejercicio de criterio más importante del lab. A partir de
# tu propio data_profiling.md (Pregunta 3: variantes de "Bogotá", y lo
# que hayas visto del resto de regiones al correr 00_profiling.py),
# construye el diccionario completo de variante -> valor canónico.
#
# Los valores canónicos son: "BOGOTÁ", "MEDELLÍN", "CALI",
# "BARRANQUILLA", "BUCARAMANGA", "OTRO" (exactamente así, mayúscula y
# con tilde donde corresponde).
#
# Una entrada de ejemplo por región (identidad + una abreviatura cada
# una) para que veas el patrón -- te falta completar el resto de las
# variantes de cada región, más toda la categoría "Otro":
MAPA_REGION = {
    # ── Bogotá ─────────────────────────────────────────────
    "BOGOTÁ": "BOGOTÁ",
    "Bogota": "BOGOTÁ",
    "bogota": "BOGOTÁ",
    "BTA": "BOGOTÁ",
    "Bta": "BOGOTÁ",
    "BOGOTA": "BOGOTÁ",
    " Bogotá": "BOGOTÁ",
    "Bogotá": "BOGOTÁ",

    # ── Medellín ───────────────────────────────────────────
    "Medellín": "MEDELLÍN",
    "MEDELLÍN": "MEDELLÍN",
    "medellin": "MEDELLÍN",
    "Medellin": "MEDELLÍN",
    "MDE": "MEDELLÍN",
    "medellín": "MEDELLÍN",

    # ── Cali ───────────────────────────────────────────────
    "CALI": "CALI",
    "Cali": "CALI",
    " Cali": "CALI",
    "CLO": "CALI",
    "cali": "CALI",

    # ── Barranquilla ───────────────────────────────────────
    "BARRANQUILLA": "BARRANQUILLA",
    "Bquilla": "BARRANQUILLA",
    "Barranquilla": "BARRANQUILLA",
    "BAQ": "BARRANQUILLA",
    "barranquilla": "BARRANQUILLA",

    # ── Bucaramanga ────────────────────────────────────────
    "BGA": "BUCARAMANGA",
    "Bucaramanga": "BUCARAMANGA",
    "Buca": "BUCARAMANGA",
    "bucaramanga": "BUCARAMANGA",
    "BUCARAMANGA": "BUCARAMANGA",

    # ── Otros ───────────────────────────────────────────────
    "Desconocido": "OTRO",
    "otro": "OTRO",
    "N/A": "OTRO",
    "NA": "OTRO",
    "OTRO": "OTRO",
}


def construir_mapa(col, mapa: dict, valor_por_defecto: str):
    """NARROW ✅: construye un solo Column expression encadenando
    when() por cada entrada del mapa -- sigue siendo una transformación
    fila a fila, sin importar cuántos when() tenga la cadena. PASO 1
    (upper+trim) resuelve mayúsculas y espacios; PASO 2 (el propio
    when-chain) mapea el resto a su valor canónico."""
    col_norm = F.upper(F.trim(col))  # PASO 1
    chain = None
    for crudo, canonico in mapa.items():  # PASO 2
        crudo_norm = crudo.strip().upper()
        condicion = col_norm == crudo_norm
        chain = F.when(condicion, F.lit(canonico)) if chain is None else chain.when(condicion, F.lit(canonico))
    return chain.otherwise(F.lit(valor_por_defecto))


# TODO: usa construir_mapa() para crear la columna "region_silver" a
# partir de la columna "region" y tu MAPA_REGION.
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica (pista:
# aunque construir_mapa() encadena decenas de when(), ¿cada fila de
# salida depende de otras filas para resolverse, o solo de sí misma?).
df_region = df_fechas.withColumn(
    "region_silver",
    construir_mapa(
        F.col("region"),
        MAPA_REGION,
        "OTRO"
    )
)

# NARROW
# La normalización de region se realiza fila por fila utilizando
# únicamente el valor de la columna region de cada registro.
# No se necesita comparar filas entre sí ni redistribuir datos entre
# particiones, por lo que es una transformación NARROW.
# PASO 3 (dado): verificación -- si tu MAPA_REGION está completo, esto
# debe imprimir exactamente 6.
n_valores_region = df_region.select("region_silver").distinct().count()
print(f"3.3 Región: {n_valores_region} valores distintos después de normalizar (debe ser 6)")
if n_valores_region != 6:
    df_region.select("region_silver").distinct().show(40, truncate=False)
    print("^ Alguno de estos valores te sobra -- te falta un alias en MAPA_REGION. "
          "Revisa especialmente las formas sin tilde.")

# TODO (documentar, no código): decide y anota en pipeline_analysis.md
# cómo manejaste 'N/A', 'NA' y 'Desconocido' -- ¿los agrupaste en
# 'OTRO' o los trataste como nulos? ¿Por qué?

# ═══════════════════════════════════════════════════════════════
# TODO 3.4 · Normalización de canal
# ═══════════════════════════════════════════════════════════════
# Mismo patrón que 3.3 (usa construir_mapa() otra vez), pero esta vez
# el valor canónico de salida es minúscula con guion bajo:
# "app_movil", "web", "tienda_fisica", "telefono" (así, exactamente).
#
# Un ejemplo para que veas el patrón:
MAPA_CANAL = {
    # ── App móvil ───────────────────────────────────────────
    "App Móvil": "app_movil",
    "móvil": "app_movil",
    "app movil": "app_movil",
    "APP MOVIL": "app_movil",
    "APP_MOVIL": "app_movil",

    # ── Web ─────────────────────────────────────────────────
    "online": "web",
    "pagina_web": "web",
    "WEB": "web",
    "sitio_web": "web",
    "Web": "web",

    # ── Tienda física ───────────────────────────────────────
    "TIENDA FISICA": "tienda_fisica",
    "Tienda Física": "tienda_fisica",
    "tienda": "tienda_fisica",
    "TIENDA": "tienda_fisica",
    "físico": "tienda_fisica",

    # ── Teléfono ────────────────────────────────────────────
    "call_center": "telefono",
    "llamada": "telefono",
    "TELEFONO": "telefono",
    "tel": "telefono",
    "Teléfono": "telefono",
}

# TODO: usa construir_mapa() para crear la columna "canal_silver" a
# partir de la columna "canal" y tu MAPA_CANAL. Usa "otro_canal" como
# valor por defecto (tercer argumento de construir_mapa()).
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica (mismo
# razonamiento que aplicaste para region_silver).
df_canal = df_region.withColumn(
    "canal_silver",
    construir_mapa(
        F.col("canal"),
        MAPA_CANAL,
        "otro_canal"
    )
)

# NARROW
# La normalización de canal se realiza fila por fila utilizando
# únicamente el valor de la columna canal de cada registro.
# No es necesario comparar filas ni redistribuir registros entre
# particiones, por lo que es una transformación NARROW.

n_valores_canal = df_canal.select("canal_silver").distinct().count()
print(f"3.4 Canal: {n_valores_canal} valores distintos después de normalizar (debe ser 4)")
if n_valores_canal != 4:
    df_canal.select("canal_silver").distinct().show(25, truncate=False)
    print("^ Alguno de estos valores te sobra -- te falta un alias en MAPA_CANAL.")

# ═══════════════════════════════════════════════════════════════
# TODO 3.5 · Validación y recálculo de total
# ═══════════════════════════════════════════════════════════════
# Regla de negocio: total_correcto = cantidad * precio_unit. El
# 'total' del raw NO se usa -- es poco confiable (nulos, negativos,
# error de escala, ver tu propio data_profiling.md Pregunta 5).
#
# TODO paso 1: castea "cantidad" y "precio_unit" a double, en columnas
# nuevas llamadas EXACTAMENTE "cantidad_num" y "precio_num".
df_cast = (
    df_canal
    .withColumn("cantidad_num", F.col("cantidad").cast("double"))
    .withColumn("precio_num", F.col("precio_unit").cast("double"))
) # TODO: reemplaza por df_canal + "cantidad_num" + "precio_num"

# TODO paso 2: filtra para quedarte solo con las filas donde
# cantidad_num > 0 AND precio_num > 0 (ambos deben existir con valor
# válido para que el recálculo tenga sentido de negocio).
df_validado = df_cast.filter(
    (F.col("cantidad_num") > 0) &
    (F.col("precio_num") > 0)
)  # TODO: reemplaza por el filtro

# TODO paso 3: agrega la columna "total_silver" =
# round(cantidad_num * precio_num, 2).
#
# Clasificación de los 3 pasos de arriba: → [tu respuesta: NARROW ✅ o
# WIDE ❌] -- justifica.
df_total = df_validado.withColumn(
    "total_silver",
    F.round(
        F.col("cantidad_num") * F.col("precio_num"),
        2
    )
)  # TODO: reemplaza por df_validado + "total_silver"

n_antes_35 = df_canal.count()
n_despues_35 = df_total.count()
n_total_valido = df_canal.filter(
    F.col("total").cast("double").isNotNull() &
    (F.col("total").cast("double") > 0)
).count()

print(f"3.5 Total: {n_antes_35:,} -> {n_despues_35:,} filas tras filtrar cantidad/precio inválidos")
print(f"Filas conservadas filtrando directamente por total válido: {n_total_valido:,}")
# Cuando termines: responde en pipeline_analysis.md (Pregunta 2)
# cuántas filas preservaste con esta estrategia vs. si hubieras
# filtrado directamente por 'total' inválido -- compáralas.

# ═══════════════════════════════════════════════════════════════
# TODO 3.6 · Normalización de tipos
# ═══════════════════════════════════════════════════════════════
# En tu profiling (Pregunta 6) viste que vendedor_id mezcla enteros
# puros, valores con prefijo "VEN-" y un tercer formato "mixto".
#
# TODO: usa F.regexp_extract() para quedarte SOLO con la parte
# numérica de "vendedor_id", sin importar el formato de entrada.
# Sobreescribe la columna "vendedor_id" con el resultado.
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica.
df_vendedor = df_total.withColumn(
    "vendedor_id",
    F.regexp_extract(
        F.col("vendedor_id"),
        r"(\d+)",
        1
    )
)

# NARROW
# regexp_extract() procesa únicamente el vendedor_id de cada fila y
# devuelve la parte numérica encontrada. No necesita comparar filas ni
# mover registros entre particiones, por lo que es NARROW.  # TODO: reemplaza por df_total con "vendedor_id" limpio

# TODO: valida "email_cliente" con una expresión regular de email
# razonable (usuario@dominio.tld) usando F.rlike(). Crea una columna
# booleana nueva llamada "email_valido". NO elimines ni pongas en null
# los emails inválidos -- solo márcalos.
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica.
df_tipos = df_vendedor.withColumn(
    "email_valido",
    F.col("email_cliente").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
)

# NARROW
# rlike() valida el email de cada fila de forma independiente. No
# necesita comparar registros ni realizar un shuffle, por lo que es
# NARROW.  # TODO: reemplaza por df_vendedor + "email_valido"

# ═══════════════════════════════════════════════════════════════
# Selección final de columnas de Silver (dado -- asume los nombres de
# columna exactos especificados en cada TODO de arriba)
# ═══════════════════════════════════════════════════════════════
df_silver = df_tipos.select(
    "pedido_id",
    F.col("fecha_parsed").alias("fecha"),
    F.col("region_silver").alias("region"),
    F.col("canal_silver").alias("canal"),
    "categoria",
    "producto",
    F.col("cantidad_num").cast("int").alias("cantidad"),
    F.col("precio_num").alias("precio_unit"),
    "total_silver",
    "vendedor_id",
    "email_cliente",
    "email_valido",
    "metodo_pago",
    "devuelto",
    "calificacion",
).filter(F.col("pedido_id").isNotNull())  # el MERGE necesita una clave no nula

# ═══════════════════════════════════════════════════════════════
# TODO 3.7 · MERGE a Silver -- ingesta incremental ACID
# ═══════════════════════════════════════════════════════════════
# Este es el syntax nuevo de esta semana. La forma general de un MERGE
# con la API de Delta en Python es:
#
#   delta_table.alias("s").merge(
#       df_nuevo.alias("n"),
#       "<condición de join sobre la clave de negocio>"
#   ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#
# TODO: completa la rama `if` de abajo usando ese patrón. La clave de
# negocio del MERGE es pedido_id (compara "s.pedido_id" contra
# "n.pedido_id"). El DataFrame nuevo es df_silver.
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica en
# términos de qué hace Spark internamente para poder decidir, fila por
# fila, si es un UPDATE o un INSERT.
if DeltaTable.isDeltaTable(spark, SILVER):
    print("3.7 Tabla Silver existe -- ejecutando MERGE")
    silver_table = DeltaTable.forPath(spark, SILVER)

    (
        silver_table.alias("s")
        .merge(
            df_silver.alias("n"),
            "s.pedido_id = n.pedido_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    # TODO: tu código de MERGE aquí (silver_table.alias("s").merge(...)....execute())
else:
    # Primera ejecución -- no hay tabla Silver todavía contra la cual
    # comparar, así que no hay MERGE la primera vez (dado).
    print("3.7 Primera ejecución -- creando tabla Silver")
    df_silver.write.format("delta").mode("overwrite").save(SILVER)

df_silver_final = spark.read.format("delta").load(SILVER)
print(f"Filas en Silver tras el MERGE: {df_silver_final.count():,}")

# ── Verificación con time travel: versión 0 vs versión actual (dado) ──
silver_table = DeltaTable.forPath(spark, SILVER)
historial = silver_table.history().select("version", "timestamp", "operation")
print("\n=== Historial de versiones de Silver ===")
historial.show(truncate=False)

version_0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER)
print(f"Versión 0: {version_0.count():,} filas")
print(f"Versión actual: {df_silver_final.count():,} filas")

# ═══════════════════════════════════════════════════════════════
# 3.8 · Plan físico -- dónde están los Exchange del MERGE (dado)
# ═══════════════════════════════════════════════════════════════
# .explain(mode="formatted") imprime el plan físico completo. Busca
# los bloques que empiezan con "Exchange" -- cada uno es un shuffle
# real. Complementa esto con la inspección visual en Spark UI (Parte
# 3.8 de ../README.md) -- el plan de texto y el DAG visual muestran la
# misma información en dos formatos.
print("\n=== Plan físico de la escritura a Silver (busca 'Exchange') ===")
df_silver.explain(mode="formatted")

spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1


"""


=== Historial de versiones de Silver ===
26/08/23 01:15:13 INFO CodeGenerator: Code generated in 4.138025 ms
26/08/23 01:15:13 INFO CodeGenerator: Code generated in 3.881421 ms
+-------+-------------------+---------+
|version|timestamp          |operation|
+-------+-------------------+---------+
|0      |2026-08-23 01:15:10|WRITE    |
+-------+-------------------+---------+

== Physical Plan ==
AdaptiveSparkPlan (7)
+- Project (6)
   +- HashAggregate (5)
      +- Exchange (4)
         +- HashAggregate (3)
            +- Filter (2)
               +- Scan parquet  (1)


(1) Scan parquet
Output [16]: [pedido_id#37, fecha#38, categoria#39, producto#40, cantidad#41, precio_unit#42, total#43, email_cliente#44, metodo_pago#45, devuelto#46, calificacion#47, region#48, canal#49, vendedor_id#50, _ingested_at#51, _source_file#52]
Batched: true
Location: PreparedDeltaFileIndex [s3a://st1630-ealvarezc1-2026/bronze/pedidos]
PushedFilters: [IsNotNull(cantidad), IsNotNull(precio_unit), IsNotNull(pedido_id)]
ReadSchema: struct<pedido_id:string,fecha:string,categoria:string,producto:string,cantidad:string,precio_unit:string,total:string,email_cliente:string,metodo_pago:string,devuelto:string,calificacion:string,region:string,canal:string,vendedor_id:string,_ingested_at:timestamp,_source_file:string>

(2) Filter
Input [16]: [pedido_id#37, fecha#38, categoria#39, producto#40, cantidad#41, precio_unit#42, total#43, email_cliente#44, metodo_pago#45, devuelto#46, calificacion#47, region#48, canal#49, vendedor_id#50, _ingested_at#51, _source_file#52]
Condition : (((((isnotnull(cantidad#41) AND isnotnull(precio_unit#42)) AND isnotnull(coalesce(cast(gettimestamp(fecha#38, yyyy/MM/dd, TimestampType, Some(UTC), false) as date), cast(gettimestamp(fecha#38, yyyy-MM-dd, TimestampType, Some(UTC), false) as date), cast(gettimestamp(fecha#38, dd-MM-yyyy, TimestampType, Some(UTC), false) as date), cast(gettimestamp(fecha#38, dd/MM/yyyy, TimestampType, Some(UTC), false) as date)))) AND (cast(cantidad#41 as double) > 0.0)) AND (cast(precio_unit#42 as double) > 0.0)) AND isnotnull(pedido_id#37))

(3) HashAggregate
Input [16]: [pedido_id#37, fecha#38, categoria#39, producto#40, cantidad#41, precio_unit#42, total#43, email_cliente#44, metodo_pago#45, devuelto#46, calificacion#47, region#48, canal#49, vendedor_id#50, _ingested_at#51, _source_file#52]
Keys [16]: [calificacion#47, categoria#39, devuelto#46, _ingested_at#51, total#43, pedido_id#37, cantidad#41, fecha#38, _source_file#52, email_cliente#44, precio_unit#42, region#48, metodo_pago#45, vendedor_id#50, canal#49, producto#40]
Functions: []
Aggregate Attributes: []
Results [16]: [calificacion#47, categoria#39, devuelto#46, _ingested_at#51, total#43, pedido_id#37, cantidad#41, fecha#38, _source_file#52, email_cliente#44, precio_unit#42, region#48, metodo_pago#45, vendedor_id#50, canal#49, producto#40]

(4) Exchange
Input [16]: [calificacion#47, categoria#39, devuelto#46, _ingested_at#51, total#43, pedido_id#37, cantidad#41, fecha#38, _source_file#52, email_cliente#44, precio_unit#42, region#48, metodo_pago#45, vendedor_id#50, canal#49, producto#40]
Arguments: hashpartitioning(calificacion#47, categoria#39, devuelto#46, _ingested_at#51, total#43, pedido_id#37, cantidad#41, fecha#38, _source_file#52, email_cliente#44, precio_unit#42, region#48, metodo_pago#45, vendedor_id#50, canal#49, producto#40, 32), ENSURE_REQUIREMENTS, [plan_id=1435]

(5) HashAggregate
Input [16]: [calificacion#47, categoria#39, devuelto#46, _ingested_at#51, total#43, pedido_id#37, cantidad#41, fecha#38, _source_file#52, email_cliente#44, precio_unit#42, region#48, metodo_pago#45, vendedor_id#50, canal#49, producto#40]
Keys [16]: [calificacion#47, categoria#39, devuelto#46, _ingested_at#51, total#43, pedido_id#37, cantidad#41, fecha#38, _source_file#52, email_cliente#44, precio_unit#42, region#48, metodo_pago#45, vendedor_id#50, canal#49, producto#40]
Functions: []
Aggregate Attributes: []
Results [13]: [pedido_id#37, categoria#39, producto#40, email_cliente#44, metodo_pago#45, devuelto#46, calificacion#47, vendedor_id#50, coalesce(cast(gettimestamp(fecha#38, yyyy/MM/dd, TimestampType, Some(UTC), false) as date), cast(gettimestamp(fecha#38, yyyy-MM-dd, TimestampType, Some(UTC), false) as date), cast(gettimestamp(fecha#38, dd-MM-yyyy, TimestampType, Some(UTC), false) as date), cast(gettimestamp(fecha#38, dd/MM/yyyy, TimestampType, Some(UTC), false) as date)) AS fecha_parsed#621, CASE WHEN (upper(trim(region#48, None)) = BOGOTÁ) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = BOGOTA) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = BOGOTA) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = BTA) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = BTA) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = BOGOTA) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = BOGOTÁ) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = BOGOTÁ) THEN BOGOTÁ WHEN (upper(trim(region#48, None)) = MEDELLÍN) THEN MEDELLÍN WHEN (upper(trim(region#48, None)) = MEDELLÍN) THEN MEDELLÍN WHEN (upper(trim(region#48, None)) = MEDELLIN) THEN MEDELLÍN WHEN (upper(trim(region#48, None)) = MEDELLIN) THEN MEDELLÍN WHEN (upper(trim(region#48, None)) = MDE) THEN MEDELLÍN WHEN (upper(trim(region#48, None)) = MEDELLÍN) THEN MEDELLÍN WHEN (upper(trim(region#48, None)) = CALI) THEN CALI WHEN (upper(trim(region#48, None)) = CALI) THEN CALI WHEN (upper(trim(region#48, None)) = CALI) THEN CALI WHEN (upper(trim(region#48, None)) = CLO) THEN CALI WHEN (upper(trim(region#48, None)) = CALI) THEN CALI WHEN (upper(trim(region#48, None)) = BARRANQUILLA) THEN BARRANQUILLA WHEN (upper(trim(region#48, None)) = BQUILLA) THEN BARRANQUILLA WHEN (upper(trim(region#48, None)) = BARRANQUILLA) THEN BARRANQUILLA WHEN (upper(trim(region#48, None)) = BAQ) THEN BARRANQUILLA WHEN (upper(trim(region#48, None)) = BARRANQUILLA) THEN BARRANQUILLA WHEN (upper(trim(region#48, None)) = BGA) THEN BUCARAMANGA WHEN (upper(trim(region#48, None)) = BUCARAMANGA) THEN BUCARAMANGA WHEN (upper(trim(region#48, None)) = BUCA) THEN BUCARAMANGA WHEN (upper(trim(region#48, None)) = BUCARAMANGA) THEN BUCARAMANGA WHEN (upper(trim(region#48, None)) = BUCARAMANGA) THEN BUCARAMANGA WHEN (upper(trim(region#48, None)) = DESCONOCIDO) THEN OTRO WHEN (upper(trim(region#48, None)) = OTRO) THEN OTRO WHEN (upper(trim(region#48, None)) = N/A) THEN OTRO WHEN (upper(trim(region#48, None)) = NA) THEN OTRO WHEN (upper(trim(region#48, None)) = OTRO) THEN OTRO ELSE OTRO END AS region_silver#745, CASE WHEN (upper(trim(canal#49, None)) = APP MÓVIL) THEN app_movil WHEN (upper(trim(canal#49, None)) = MÓVIL) THEN app_movil WHEN (upper(trim(canal#49, None)) = APP MOVIL) THEN app_movil WHEN (upper(trim(canal#49, None)) = APP MOVIL) THEN app_movil WHEN (upper(trim(canal#49, None)) = APP_MOVIL) THEN app_movil WHEN (upper(trim(canal#49, None)) = ONLINE) THEN web WHEN (upper(trim(canal#49, None)) = PAGINA_WEB) THEN web WHEN (upper(trim(canal#49, None)) = WEB) THEN web WHEN (upper(trim(canal#49, None)) = SITIO_WEB) THEN web WHEN (upper(trim(canal#49, None)) = WEB) THEN web WHEN (upper(trim(canal#49, None)) = TIENDA FISICA) THEN tienda_fisica WHEN (upper(trim(canal#49, None)) = TIENDA FÍSICA) THEN tienda_fisica WHEN (upper(trim(canal#49, None)) = TIENDA) THEN tienda_fisica WHEN (upper(trim(canal#49, None)) = TIENDA) THEN tienda_fisica WHEN (upper(trim(canal#49, None)) = FÍSICO) THEN tienda_fisica WHEN (upper(trim(canal#49, None)) = CALL_CENTER) THEN telefono WHEN (upper(trim(canal#49, None)) = LLAMADA) THEN telefono WHEN (upper(trim(canal#49, None)) = TELEFONO) THEN telefono WHEN (upper(trim(canal#49, None)) = TEL) THEN telefono WHEN (upper(trim(canal#49, None)) = TELÉFONO) THEN telefono ELSE otro_canal END AS canal_silver#856, cast(cantidad#41 as double) AS cantidad_num#968, cast(precio_unit#42 as double) AS precio_num#989]

(6) Project
Output [15]: [pedido_id#37, fecha_parsed#621 AS fecha#1300, region_silver#745 AS region#1301, canal_silver#856 AS canal#1302, categoria#39, producto#40, cast(cantidad_num#968 as int) AS cantidad#1303, precio_num#989 AS precio_unit#1304, round((cantidad_num#968 * precio_num#989), 2) AS total_silver#1011, regexp_extract(vendedor_id#50, (\d+), 1) AS vendedor_id#1253, email_cliente#44, RLIKE(email_cliente#44, ^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$) AS email_valido#1276, metodo_pago#45, devuelto#46, calificacion#47]
Input [13]: [pedido_id#37, categoria#39, producto#40, email_cliente#44, metodo_pago#45, devuelto#46, calificacion#47, vendedor_id#50, fecha_parsed#621, region_silver#745, canal_silver#856, cantidad_num#968, precio_num#989]

(7) AdaptiveSparkPlan
Output [15]: [pedido_id#37, fecha#1300, region#1301, canal#1302, categoria#39, producto#40, cantidad#1303, precio_unit#1304, total_silver#1011, vendedor_id#1253, email_cliente#44, email_valido#1276, metodo_pago#45, devuelto#46, calificacion#47]
Arguments: isFinalPlan=false



"""