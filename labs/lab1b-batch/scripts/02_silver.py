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
BUCKET = "st1630-samedinac-2023"  # EDITAR: el mismo bucket del Lab 1a
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
# ─────────────────────────────────────────────────────────────

df_bronze = spark.read.format("delta").load(BRONZE)
n_bronze = df_bronze.count()
print(f"Filas en Bronze: {n_bronze:,}")

# ═══════════════════════════════════════════════════════════════
# 3.1 · Deduplicación (dado)
# ═══════════════════════════════════════════════════════════════
# Clasificación: → WIDE ❌ -- dropDuplicates() hashea la fila completa y
# reparticiona por ese hash (Exchange) para que dos filas idénticas,
# que pudieron llegar en particiones distintas, se comparen en el
# mismo executor. Hay shuffle físico aunque no haya groupBy ni join.
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
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "dd-MM-yyyy",
    "dd/MM/yyyy",  # Colombia: gana en fechas ambiguas (04/11 = 4 de noviembre)
    "MM/dd/yyyy",  # fallback US (p. ej. 01/20/2026, día 20 imposible en dd/MM)
]

# TODO: usa F.coalesce(...) combinando un F.to_date(F.col("fecha"), fmt)
# por cada formato de FORMATOS_FECHA, y guarda el resultado en una
# columna nueva llamada EXACTAMENTE "fecha_parsed" (withColumn).
#
# Clasificación: → NARROW ✅ -- to_date/coalesce son fila a fila; un
# parseo que falla devuelve null y el siguiente formato se intenta en
# la misma partición, sin shuffle.
df_fechas = df_dedup.withColumn(
    "fecha_parsed",
    F.coalesce(*[F.to_date(F.col("fecha"), fmt) for fmt in FORMATOS_FECHA]),
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
    "Bogotá": "BOGOTÁ",       # ejemplo: la forma "ya correcta" también necesita estar en el mapa
    "BTA": "BOGOTÁ",           # ejemplo: abreviatura de Bogotá
    "MDE": "MEDELLÍN",         # ejemplo: abreviatura de Medellín
    "CLO": "CALI",             # ejemplo: abreviatura de Cali (código de aeropuerto)
    "BAQ": "BARRANQUILLA",     # ejemplo: abreviatura de Barranquilla (código de aeropuerto)
    "BGA": "BUCARAMANGA",      # ejemplo: abreviatura de Bucaramanga (código de aeropuerto)
    # TODO: agrega aquí el resto de las variantes que encontraste en tu
    # profiling para las 6 regiones -- Bogotá, Medellín, Cali,
    # Barranquilla, Bucaramanga y Otro. Ojo con los acentos: upper()
    # NO le quita la tilde a una palabra, así que "BOGOTA" (sin tilde)
    # y "BOGOTÁ" (con tilde) son dos entradas DISTINTAS que ambas
    # necesitan estar en el mapa si tu dataset trae las dos formas.
    "BOGOTÁ": "BOGOTÁ",
    "Bogota": "BOGOTÁ",
    "bogota": "BOGOTÁ",
    "BOGOTA": "BOGOTÁ",
    "Bta": "BOGOTÁ",
    "Medellín": "MEDELLÍN",
    "MEDELLÍN": "MEDELLÍN",
    "medellín": "MEDELLÍN",
    "medellin": "MEDELLÍN",
    "Medellin": "MEDELLÍN",
    "CALI": "CALI",
    "Cali": "CALI",
    "cali": "CALI",
    "BARRANQUILLA": "BARRANQUILLA",
    "Barranquilla": "BARRANQUILLA",
    "barranquilla": "BARRANQUILLA",
    "Bquilla": "BARRANQUILLA",
    "BUCARAMANGA": "BUCARAMANGA",
    "Bucaramanga": "BUCARAMANGA",
    "bucaramanga": "BUCARAMANGA",
    "Buca": "BUCARAMANGA",
    "OTRO": "OTRO",
    "otro": "OTRO",
    "N/A": "OTRO",
    "NA": "OTRO",
    "Desconocido": "OTRO",
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
# Clasificación: → NARROW ✅ -- cada fila se resuelve sola con upper+trim
# y la cadena de when(); más when() no implica shuffle.
df_region = df_fechas.withColumn(
    "region_silver",
    construir_mapa(F.col("region"), MAPA_REGION, "OTRO"),
)

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
    "APP_MOVIL": "app_movil",  # ejemplo
    # TODO: agrega aquí el resto de las variantes que encontraste en tu
    # profiling (Pregunta 4: variantes de "app_movil", y lo que hayas
    # visto del resto de canales) para los 4 canales: app_movil, web,
    # tienda_fisica, telefono.
    "App Móvil": "app_movil",
    "móvil": "app_movil",
    "app movil": "app_movil",
    "APP MOVIL": "app_movil",
    "online": "web",
    "pagina_web": "web",
    "WEB": "web",
    "sitio_web": "web",
    "Web": "web",
    "TIENDA FISICA": "tienda_fisica",
    "Tienda Física": "tienda_fisica",
    "tienda": "tienda_fisica",
    "TIENDA": "tienda_fisica",
    "físico": "tienda_fisica",
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
# Clasificación: → NARROW ✅ -- mismo razonamiento que region_silver:
# mapeo fila a fila, sin Exchange.
df_canal = df_region.withColumn(
    "canal_silver",
    construir_mapa(F.col("canal"), MAPA_CANAL, "otro_canal"),
)

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
# Clasificación de los 3 pasos de arriba: → NARROW ✅ -- cast, filter y
# round son predicados/aritmética por fila; el filtro no reune filas
# entre executors.
df_total = df_validado.withColumn(
    "total_silver",
    F.round(F.col("cantidad_num") * F.col("precio_num"), 2),
)

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
# Clasificación: → NARROW ✅ -- regexp_extract opera sobre el string de
# cada fila; no necesita datos de otras particiones.
df_vendedor = df_total.withColumn(
    "vendedor_id",
    F.regexp_extract(F.col("vendedor_id"), r"(\d+)", 1),
)

# TODO: valida "email_cliente" con una expresión regular de email
# razonable (usuario@dominio.tld) usando F.rlike(). Crea una columna
# booleana nueva llamada "email_valido". NO elimines ni pongas en null
# los emails inválidos -- solo márcalos.
#
# Clasificación: → NARROW ✅ -- rlike es una predicado por fila.
df_tipos = df_vendedor.withColumn(
    "email_valido",
    F.col("email_cliente").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
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
# Clasificación: → WIDE ❌ -- el MERGE compara df_silver contra la tabla
# existente por pedido_id; Spark tiene que shufflear ambas partes por
# esa clave (Exchange) para decidir UPDATE vs INSERT.
if DeltaTable.isDeltaTable(spark, SILVER):
    print("3.7 Tabla Silver existe -- ejecutando MERGE")
    silver_table = DeltaTable.forPath(spark, SILVER)
    silver_table.alias("s").merge(
        df_silver.alias("n"),
        "s.pedido_id = n.pedido_id",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
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
