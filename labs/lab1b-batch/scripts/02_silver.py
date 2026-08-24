"""02_silver.py — Lab 1b (ST1630-2026-2, S5-S6)

Bronze -> Silver: limpieza, normalización y la primera escritura ACID
de verdad del lab (el MERGE). Este es el script central del laboratorio
-- cada paso está numerado igual que la Parte 3 de ../README.md.

Versión completada: incluye los cinco formatos de fecha, los mapas de
las 35 variantes de región y las 20 variantes de canal, la validación
del total, la normalización de tipos y el MERGE incremental en Delta.

IMPORTANTE -- contratos de nombres de columna: la selección final
`df_silver = df_tipos.select(...)` asume los nombres producidos en
cada etapa. Si se cambian, también debe ajustarse esa selección.

Uso en un clúster creado con Delta habilitado:
    spark-submit 02_silver.py

Si el clúster de Lab 1a se creó solo con Spark/Hadoop, carga los JAR
Delta que EMR 6.15 ya instala localmente:
    spark-submit --jars /usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar 02_silver.py

Qué puedes delegar: sintaxis puntual (¿cómo se llama la función de
regex en PySpark?). Qué NO puedes delegar: el contenido de
MAPA_REGION/MAPA_CANAL (sale de TU profiling, no del de nadie más), la
estrategia de validación de 'total', y la clasificación NARROW/WIDE de
cada bloque que completes -- ver ../README.md, "Bitácora de delegación".
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("ST1630-Lab1b-Silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
# Las operaciones WIDE de este script (deduplicación, distinct y MERGE)
# usan este número de particiones de shuffle. Con 32 se aprovechan los
# 4 executors x 8 cores del clúster sin generar 200 particiones pequeñas.
spark.conf.set("spark.sql.shuffle.partitions", "32")

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-ssalazarh3-2026"  # EDITAR: el mismo bucket del Lab 1a
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
# ─────────────────────────────────────────────────────────────

# Leer Delta garantiza que Spark use únicamente los archivos Parquet
# activos según `_delta_log`, en vez de listar objetos de S3 sin conocer
# qué versión lógica de la tabla representan. El scan es NARROW ✅.
df_bronze = spark.read.format("delta").load(BRONZE)

# count() es una acción: obliga a ejecutar la lectura y combina los
# conteos parciales para producir un único total en el driver.
n_bronze = df_bronze.count()
print(f"Filas en Bronze: {n_bronze:,}")

# ═══════════════════════════════════════════════════════════════
# 3.1 · Deduplicación (dado)
# ═══════════════════════════════════════════════════════════════
# Las columnas de auditoría cambian entre ingestas, aunque el pedido
# crudo sea el mismo. Por eso la igualdad se evalúa sobre columnas de
# negocio y no sobre `_ingested_at`/`_source_file`; así una reejecución
# accidental de Bronze no duplica lógicamente Silver.
COLUMNAS_NEGOCIO = [
    columna
    for columna in df_bronze.columns
    if not columna.startswith("_")
]

# Clasificación: WIDE. Para decidir si dos filas son idénticas,
# Spark debe reunir en la misma partición las filas con las mismas
# columnas de negocio. `dropDuplicates()` provoca un shuffle físico,
# aunque no aparezca explícitamente un groupBy ni un join.
df_dedup = df_bronze.dropDuplicates(COLUMNAS_NEGOCIO)
n_dedup = df_dedup.count()
print(f"3.1 Deduplicación: {n_bronze:,} -> {n_dedup:,} filas (-{n_bronze - n_dedup:,} duplicados)")

# ═══════════════════════════════════════════════════════════════
# 3.2 · Fechas -- el reto de los 5 formatos
# ═══════════════════════════════════════════════════════════════
# En tu data_profiling.md (Pregunta 2) ya identificaste los 5 formatos
# de fecha del dataset. Vas a necesitar el nombre de patrón de Spark
# para cada uno -- revisa la documentación de `to_date()` si no
# recuerdas la sintaxis de los patrones (p. ej. "yyyy-MM-dd").
#
# Se conserva el orden identificado en el profiling. En los valores
# ambiguos con día y mes <= 12 se prioriza dd/MM/yyyy sobre MM/dd/yyyy;
# esta es una decisión explícita porque el texto original, por sí solo,
# no permite desambiguarlos con certeza.
FORMATOS_FECHA = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "dd-MM-yyyy",
    "MM/dd/yyyy",
    "yyyy/MM/dd",
]

if len(FORMATOS_FECHA) != 5:
    raise ValueError(
        "Completa FORMATOS_FECHA con los 5 patrones identificados en tu profiling "
        "antes de ejecutar Silver."
    )

# Clasificación: NARROW. Cada fecha se interpreta usando únicamente
# el valor de su propia fila; no se intercambian datos entre executors.
# Aquí `F.coalesce` es una expresión SQL que elige el primer parseo no
# nulo; no debe confundirse con `DataFrame.coalesce()`, que cambia el
# número de particiones.
df_fechas = df_dedup.withColumn(
    "fecha_parsed",
    F.coalesce(*[F.to_date(F.col("fecha"), formato) for formato in FORMATOS_FECHA]),
)

n_sin_fecha = df_fechas.filter(F.col("fecha_parsed").isNull()).count()
print(f"3.2 Fechas: {n_sin_fecha:,} filas sin ningún formato reconocido (se descartan)")
df_fechas = df_fechas.filter(F.col("fecha_parsed").isNotNull())

# Nota: 'dd/MM/yyyy' y 'MM/dd/yyyy' son ambiguos para días <= 12 -- el
# orden de tu lista decide cuál gana, no hay forma de saberlo con
# certeza solo con el dato. Si te interesa, coméntalo en
# pipeline_analysis.md (no es una de las 5 preguntas obligatorias, pero
# demuestra que entendiste la limitación).

# ═══════════════════════════════════════════════════════════════
# 3.3 · Normalización de región -- el reto principal
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
# Inventario completo de las 35 variantes generadas para las seis
# categorías canónicas:
MAPA_REGION = {
    # Bogotá (8 variantes)
    "Bogotá": "BOGOTÁ",
    "bogota": "BOGOTÁ",
    "BOGOTÁ": "BOGOTÁ",
    "Bogota ": "BOGOTÁ",
    " Bogotá": "BOGOTÁ",
    "BOGOTA": "BOGOTÁ",
    "Bta": "BOGOTÁ",
    "BTA": "BOGOTÁ",
    # Medellín (6 variantes)
    "Medellín": "MEDELLÍN",
    "medellin": "MEDELLÍN",
    "MEDELLÍN": "MEDELLÍN",
    "Medellin ": "MEDELLÍN",
    "MDE": "MEDELLÍN",
    "medellín": "MEDELLÍN",
    # Cali (6 variantes)
    "Cali": "CALI",
    "CALI": "CALI",
    "cali": "CALI",
    "cali ": "CALI",
    " Cali": "CALI",
    "CLO": "CALI",
    # Barranquilla (5 variantes)
    "Barranquilla": "BARRANQUILLA",
    "BARRANQUILLA": "BARRANQUILLA",
    "barranquilla": "BARRANQUILLA",
    "Bquilla": "BARRANQUILLA",
    "BAQ": "BARRANQUILLA",
    # Bucaramanga (5 variantes)
    "Bucaramanga": "BUCARAMANGA",
    "BUCARAMANGA": "BUCARAMANGA",
    "bucaramanga": "BUCARAMANGA",
    "BGA": "BUCARAMANGA",
    "Buca": "BUCARAMANGA",
    # Otros o valores sin una ciudad identificable (5 variantes)
    "OTRO": "OTRO",
    "otro": "OTRO",
    "N/A": "OTRO",
    "NA": "OTRO",
    "Desconocido": "OTRO",
}

# Esta validación falla rápido si alguien reemplaza accidentalmente el
# inventario completo por los ejemplos mínimos del enunciado.
if len(MAPA_REGION) <= 6:
    raise ValueError(
        "MAPA_REGION todavía contiene solo los ejemplos del enunciado. "
        "Complétalo a partir de tu profiling antes de ejecutar Silver."
    )

REGION_POR_DEFECTO = "OTRO"


def construir_mapa(col, mapa: dict, valor_por_defecto: str):
    """NARROW: construye un solo Column expression encadenando
    when() por cada entrada del mapa -- sigue siendo una transformación
    fila a fila, sin importar cuántos when() tenga la cadena. PASO 1
    (upper+trim) resuelve mayúsculas y espacios; PASO 2 (el propio
    when-chain) mapea el resto a su valor canónico."""
    col_norm = F.upper(F.trim(col))  # PASO 1
    chain = None
    for crudo, canonico in mapa.items():  # PASO 2
        crudo_norm = crudo.strip().upper()
        condicion = col_norm == crudo_norm
        chain = (
            F.when(condicion, F.lit(canonico))
            if chain is None
            else chain.when(condicion, F.lit(canonico))
        )
    return chain.otherwise(F.lit(valor_por_defecto))


# Clasificación: NARROW. Aunque `construir_mapa()` encadena varios
# `when`, la región de cada fila se obtiene exclusivamente a partir del
# valor de esa misma fila, sin shuffle.
df_region = df_fechas.withColumn(
    "region_silver",
    construir_mapa(F.col("region"), MAPA_REGION, REGION_POR_DEFECTO),
)

# Verificación WIDE: distinct() redistribuye los valores para poder
# eliminar repetidos. No construye la columna, solo comprueba que el
# catálogo completo redujo las 35 variantes a 6 valores canónicos.
n_valores_region = df_region.select("region_silver").distinct().count()
print(
    "3.3 Región: "
    f"{n_valores_region} valores distintos después de normalizar "
    "(debe ser 6)"
)
if n_valores_region != 6:
    df_region.select("region_silver").distinct().show(40, truncate=False)
    print(
        "^ Alguno de estos valores te sobra -- te falta un alias en "
        "MAPA_REGION. Revisa especialmente las formas sin tilde."
    )

# Decisión: `N/A`, `NA` y `Desconocido` se agrupan en `OTRO`. Se
# conservan las filas porque esos valores indican una región no
# identificada, pero no invalidan por sí mismos el resto del pedido.
# Una variante verdaderamente nueva también cae en OTRO; en producción
# convendría registrarla en una tabla de cuarentena y actualizar una
# dimensión de aliases externa, en vez de ocultarla silenciosamente.

# ═══════════════════════════════════════════════════════════════
# 3.4 · Normalización de canal
# ═══════════════════════════════════════════════════════════════
# Mismo patrón que 3.3 (usa construir_mapa() otra vez), pero esta vez
# el valor canónico de salida es minúscula con guion bajo:
# "app_movil", "web", "tienda_fisica", "telefono" (así, exactamente).
#
# Inventario completo de las 20 variantes del generador:
MAPA_CANAL = {
    # Aplicación móvil (5 variantes)
    "App Móvil": "app_movil",
    "APP_MOVIL": "app_movil",
    "app movil": "app_movil",
    "móvil": "app_movil",
    "APP MOVIL": "app_movil",
    # Web (5 variantes)
    "WEB": "web",
    "Web": "web",
    "sitio_web": "web",
    "online": "web",
    "pagina_web": "web",
    # Tienda física (5 variantes)
    "Tienda Física": "tienda_fisica",
    "TIENDA": "tienda_fisica",
    "tienda": "tienda_fisica",
    "físico": "tienda_fisica",
    "TIENDA FISICA": "tienda_fisica",
    # Teléfono (5 variantes)
    "Teléfono": "telefono",
    "TELEFONO": "telefono",
    "call_center": "telefono",
    "tel": "telefono",
    "llamada": "telefono",
}

if len(MAPA_CANAL) <= 1:
    raise ValueError(
        "MAPA_CANAL todavía contiene solo el ejemplo del enunciado. "
        "Complétalo a partir de tu profiling antes de ejecutar Silver."
    )

# Clasificación: NARROW. El canal normalizado depende únicamente del
# canal de la misma fila; la transformación no requiere shuffle.
df_canal = df_region.withColumn(
    "canal_silver",
    construir_mapa(F.col("canal"), MAPA_CANAL, "otro_canal"),
)

# Esta comprobación es WIDE por el distinct. El resultado esperado es
# cuatro categorías, no las veinte representaciones del origen.
n_valores_canal = df_canal.select("canal_silver").distinct().count()
print(
    "3.4 Canal: "
    f"{n_valores_canal} valores distintos después de normalizar "
    "(debe ser 4)"
)
if n_valores_canal != 4:
    df_canal.select("canal_silver").distinct().show(25, truncate=False)
    print("^ Alguno de estos valores te sobra -- te falta un alias en MAPA_CANAL.")

# ═══════════════════════════════════════════════════════════════
# 3.5 · Validación y recálculo de total
# ═══════════════════════════════════════════════════════════════
# Regla de negocio: total_correcto = cantidad * precio_unit. El
# 'total' del raw NO se usa -- es poco confiable (nulos, negativos,
# error de escala, ver tu propio data_profiling.md Pregunta 5).
#
# Paso 1: convertir los dos operandos a tipo numérico.
df_cast = (
    df_canal
    .withColumn("cantidad_num", F.col("cantidad").cast("double"))
    .withColumn("precio_num", F.col("precio_unit").cast("double"))
)

# Paso 2: conservar únicamente operandos existentes y positivos. Las
# comparaciones también descartan los null, porque no evalúan a true.
df_validado = df_cast.filter(
    (F.col("cantidad_num") > 0)
    & (F.col("precio_num") > 0)
)

# Paso 3: recalcular el total confiable sin usar el total del raw.
# Clasificación de los tres pasos: NARROW. El cast, el filtro y el
# cálculo trabajan fila por fila y no necesitan mover datos entre
# particiones.
df_total = df_validado.withColumn(
    "total_silver",
    F.round(F.col("cantidad_num") * F.col("precio_num"), 2),
)

n_antes_35 = df_canal.count()
n_despues_35 = df_total.count()
print(
    f"3.5 Total: {n_antes_35:,} -> {n_despues_35:,} filas tras "
    "filtrar cantidad/precio inválidos"
)

# Cuando termines: responde en pipeline_analysis.md (Pregunta 2)
# cuántas filas preservaste con esta estrategia vs. si hubieras
# filtrado directamente por 'total' inválido -- compáralas.

# ═══════════════════════════════════════════════════════════════
# 3.6 · Normalización de tipos
# ═══════════════════════════════════════════════════════════════
# En tu profiling (Pregunta 6) viste que vendedor_id mezcla enteros
# puros, valores con prefijo "VEN-" y un tercer formato "mixto".
#
# Clasificación: NARROW. La expresión regular se aplica de forma
# independiente al identificador de cada fila. Se conserva solo la
# secuencia numérica para unificar `9240`, `VEN-9240` y `v9240`.
df_vendedor = df_total.withColumn(
    "vendedor_id",
    F.regexp_extract(F.col("vendedor_id"), r"(\d+)", 1),
)

# Clasificación: NARROW. Cada correo se valida usando solo el texto
# almacenado en su propia fila. Se marca en lugar de eliminarse porque
# un email incorrecto no invalida necesariamente la transacción y la
# bandera permite medir calidad o corregirlo posteriormente.
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

df_tipos = df_vendedor.withColumn(
    "email_valido",
    F.col("email_cliente").isNotNull()
    & F.col("email_cliente").rlike(EMAIL_PATTERN),
)

# ═══════════════════════════════════════════════════════════════
# Selección final de columnas de Silver (dado -- asume los nombres de
# columna exactos producidos por las etapas anteriores)
# ═══════════════════════════════════════════════════════════════
# select, alias, cast y el filtro de pedido_id son NARROW. Además de
# cantidad y precio, aquí se materializan los tipos que Gold necesita:
# `devuelto` como boolean para calcular una tasa y `calificacion` como
# entero para obtener promedios sin depender de casts implícitos.
#
# La clave nula sí se descarta porque un MERGE no puede identificar de
# forma determinista qué registro debe actualizar.
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
    F.col("devuelto").cast("boolean").alias("devuelto"),
    F.col("calificacion").cast("int").alias("calificacion"),
).filter(F.col("pedido_id").isNotNull())  # el MERGE necesita una clave no nula

# ═══════════════════════════════════════════════════════════════
# 3.7 · MERGE a Silver -- ingesta incremental ACID
# ═══════════════════════════════════════════════════════════════
# Este es el syntax nuevo de esta semana. La forma general de un MERGE
# con la API de Delta en Python es:
#
#   delta_table.alias("s").merge(
#       df_nuevo.alias("n"),
#       "<condición de join sobre la clave de negocio>"
#   ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#
# Clasificación: WIDE. El MERGE debe comparar las claves de la tabla
# existente con las del DataFrame nuevo. Internamente realiza una
# operación equivalente a un join y puede requerir Exchange/shuffle
# para decidir qué filas actualizar y cuáles insertar.
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
    # comparar, así que no hay MERGE la primera vez. `overwrite` es
    # apropiado únicamente para crear esta tabla derivada inicial.
    print("3.7 Primera ejecución -- creando tabla Silver")
    df_silver.write.format("delta").mode("overwrite").save(SILVER)

df_silver_final = spark.read.format("delta").load(SILVER)
print(f"Filas en Silver tras el MERGE: {df_silver_final.count():,}")

# ── Verificación con time travel: versión 0 vs versión actual ──────
# Delta conserva un historial transaccional. `versionAsOf=0` reconstruye
# la instantánea inicial usando el log, sin necesitar una copia manual
# completa de la tabla.
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
# `df_silver.explain()` muestra la línea de transformaciones que produjo
# el DataFrame de entrada a la escritura, incluida la deduplicación. El
# MERGE ya fue ejecutado por Delta y su plan completo se inspecciona en
# la pestaña SQL de Spark UI. En ambos casos, un nodo `Exchange` señala
# una frontera de shuffle (shuffle write + shuffle read).
print("\n=== Plan físico de las transformaciones Silver (busca 'Exchange') ===")
df_silver.explain(mode="formatted")

spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1