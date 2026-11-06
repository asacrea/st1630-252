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
    python3 02_silver.py
    # o: spark-submit 02_silver.py

Prerequisito de entorno: pip install pyspark delta-spark==2.4.0 (ver
../README.md, Prerequisito -- misma versión que 01_bronze.py, debe
coincidir con el Spark de tu clúster EMR).

Qué puedes delegar: sintaxis puntual (¿cómo se llama la función de
regex en PySpark?). Qué NO puedes delegar: el contenido de
MAPA_REGION/MAPA_CANAL (sale de TU profiling, no del de nadie más), la
estrategia de validación de 'total', y la clasificación NARROW/WIDE de
cada bloque que completes -- ver ../README.md, "Bitácora de delegación".
"""

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_builder = (
    SparkSession.builder.appName("ST1630-Lab1b-Silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(_builder).getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "32")  # clúster del curso: 4 executors x 8 cores
# Un formato que no calza debe dar null (no SparkUpgradeException) para que coalesce() pruebe el siguiente
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-jjgomezv2-2026"  # EDITAR: el mismo bucket del Lab 1a
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
# dropDuplicates: WIDE. Para saber si dos filas son iguales tienen que llegar a la misma partición,
# así que Spark hace un shuffle por hash de todas las columnas (un Exchange) aunque no haya groupBy ni join.
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
# dd/MM/yyyy antes que MM/dd/yyyy: datos de ventas en Colombia (día/mes/año);
# los 16.033 casos con ambos campos <= 12 son indistinguibles y quedan como dd/MM.
FORMATOS_FECHA = ["yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy", "dd/MM/yyyy", "MM/dd/yyyy"]

# TODO: usa F.coalesce(...) combinando un F.to_date(F.col("fecha"), fmt)
# por cada formato de FORMATOS_FECHA, y guarda el resultado en una
# columna nueva llamada EXACTAMENTE "fecha_parsed" (withColumn).
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica.
# fecha_parsed: NARROW. Cada fila se parsea sola con los formatos, sin mirar otras filas.
df_fechas = df_dedup.withColumn(
    "fecha_parsed",
    F.coalesce(*[F.to_date(F.col("fecha"), fmt) for fmt in FORMATOS_FECHA]),
)

n_sin_fecha = df_fechas.filter(F.col("fecha_parsed").isNull()).count()
print(f"3.2 Fechas: {n_sin_fecha:,} filas sin ningún formato reconocido (se descartan)")
df_fechas = df_fechas.filter(F.col("fecha_parsed").isNotNull())

# Corrección de fechas ambiguas (NN/NN/yyyy con ambos campos <= 12): si
# leerlas como dd/MM las deja después de la fecha máxima que muestran las
# fechas NO ambiguas, esa lectura es imposible -> se reinterpretan como MM/dd.
es_ambigua = (
    F.col("fecha").rlike(r"^\d{2}/\d{2}/\d{4}$")
    & (F.substring("fecha", 1, 2).cast("int") <= 12)
    & (F.substring("fecha", 4, 2).cast("int") <= 12)
)
fecha_max_confiable = df_fechas.filter(~es_ambigua).agg(F.max("fecha_parsed")).first()[0]
corregir = es_ambigua & (F.col("fecha_parsed") > F.lit(fecha_max_confiable))
n_corregidas = df_fechas.filter(corregir).count()
df_fechas = df_fechas.withColumn(
    "fecha_parsed",
    F.when(corregir, F.to_date(F.col("fecha"), "MM/dd/yyyy")).otherwise(F.col("fecha_parsed")),
)
print(f"3.2 Fechas: fecha máxima confiable (formatos no ambiguos) = {fecha_max_confiable}; "
      f"{n_corregidas:,} fechas ambiguas reinterpretadas como MM/dd/yyyy")
print(f"3.2 Fechas: rango final {df_fechas.agg(F.min('fecha_parsed'), F.max('fecha_parsed')).first()}")

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
    # Llaves ya en la forma upper(trim()) -- construir_mapa() normaliza antes de comparar.
    "BOGOTA": "BOGOTÁ",
    "BOGOTÁ": "BOGOTÁ",
    "BTA": "BOGOTÁ",
    "MEDELLÍN": "MEDELLÍN",
    "MEDELLIN": "MEDELLÍN",
    "MDE": "MEDELLÍN",
    "CALI": "CALI",
    "CLO": "CALI",
    "BARRANQUILLA": "BARRANQUILLA",
    "BQUILLA": "BARRANQUILLA",
    "BAQ": "BARRANQUILLA",
    "BUCARAMANGA": "BUCARAMANGA",
    "BGA": "BUCARAMANGA",
    "BUCA": "BUCARAMANGA",
    "OTRO": "OTRO",
    # Región no identificada -> OTRO (ver pipeline_analysis.md, decisiones de diseño)
    "DESCONOCIDO": "OTRO",
    "N/A": "OTRO",
    "NA": "OTRO",
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
# region_silver: NARROW. Los when() encadenados solo miran el valor de esa misma fila.
df_region = df_fechas.withColumn("region_silver", construir_mapa(F.col("region"), MAPA_REGION, "SIN_MAPEO"))  # sentinel: si aparece, falta un alias

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
    "APP MOVIL": "app_movil",
    "APP MÓVIL": "app_movil",
    "APP_MOVIL": "app_movil",
    "MÓVIL": "app_movil",
    "WEB": "web",
    "ONLINE": "web",
    "PAGINA_WEB": "web",
    "SITIO_WEB": "web",
    "TIENDA": "tienda_fisica",
    "TIENDA FISICA": "tienda_fisica",
    "TIENDA FÍSICA": "tienda_fisica",
    "FÍSICO": "tienda_fisica",
    "TELEFONO": "telefono",
    "TELÉFONO": "telefono",
    "TEL": "telefono",
    "LLAMADA": "telefono",
    "CALL_CENTER": "telefono",
}

# TODO: usa construir_mapa() para crear la columna "canal_silver" a
# partir de la columna "canal" y tu MAPA_CANAL. Usa "otro_canal" como
# valor por defecto (tercer argumento de construir_mapa()).
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica (mismo
# razonamiento que aplicaste para region_silver).
# canal_silver: NARROW. Mismo caso que region, cada fila se resuelve por sí sola.
df_canal = df_region.withColumn("canal_silver", construir_mapa(F.col("canal"), MAPA_CANAL, "otro_canal"))

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
)

# TODO paso 2: filtra para quedarte solo con las filas donde
# cantidad_num > 0 AND precio_num > 0 (ambos deben existir con valor
# válido para que el recálculo tenga sentido de negocio).
df_validado = df_cast.filter((F.col("cantidad_num") > 0) & (F.col("precio_num") > 0))

# TODO paso 3: agrega la columna "total_silver" =
# round(cantidad_num * precio_num, 2).
#
# Clasificación de los 3 pasos de arriba: → [tu respuesta: NARROW ✅ o
# WIDE ❌] -- justifica.
# cast, filtro y total_silver: NARROW. Son operaciones fila por fila que se aplican dentro de cada partición.
df_total = df_validado.withColumn("total_silver", F.round(F.col("cantidad_num") * F.col("precio_num"), 2))

n_antes_35 = df_canal.count()
n_despues_35 = df_total.count()
print(f"3.5 Total: {n_antes_35:,} -> {n_despues_35:,} filas tras filtrar cantidad/precio inválidos")

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
# vendedor_id (regexp_extract): NARROW. Extrae los dígitos de cada valor sin depender de otras filas.
df_vendedor = df_total.withColumn("vendedor_id", F.regexp_extract(F.col("vendedor_id"), r"(\d+)", 1))

# TODO: valida "email_cliente" con una expresión regular de email
# razonable (usuario@dominio.tld) usando F.rlike(). Crea una columna
# booleana nueva llamada "email_valido". NO elimines ni pongas en null
# los emails inválidos -- solo márcalos.
#
# Clasificación: → [tu respuesta: NARROW ✅ o WIDE ❌] -- justifica.
# email_valido (rlike): NARROW. Valida cada email por separado, sin mover datos.
EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
df_tipos = df_vendedor.withColumn(
    "email_valido",
    F.coalesce(F.col("email_cliente").rlike(EMAIL_PATTERN), F.lit(False)),
)

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
# MERGE: WIDE. Para decidir si cada fila es UPDATE o INSERT, Spark hace un join entre df_silver y la tabla existente por pedido_id,
# lo que obliga a reparticionar ambos lados por esa clave (shuffle).
if DeltaTable.isDeltaTable(spark, SILVER):
    print("3.7 Tabla Silver existe -- ejecutando MERGE")
    silver_table = DeltaTable.forPath(spark, SILVER)
    (
        silver_table.alias("s")
        .merge(df_silver.alias("n"), "s.pedido_id = n.pedido_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
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
