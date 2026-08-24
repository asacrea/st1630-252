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
spark.conf.set("spark.sql.shuffle.partitions", "32")

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-nvcardozaa-2026"
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
# ─────────────────────────────────────────────────────────────

df_bronze = spark.read.format("delta").load(BRONZE)
n_bronze = df_bronze.count()
print(f"Filas en Bronze: {n_bronze:,}")

# ═══════════════════════════════════════════════════════════════
# 3.1 · Deduplicación (dado)
# ═══════════════════════════════════════════════════════════════
# Clasificación: dropDuplicates() requiere comparar filas para determinar cuáles son duplicadas. Para realizar esta comparación Spark puede necesitar redistribuir las filas entre particiones mediante un shuffle, de manera que las filas que pueden ser iguales queden juntas y puedan compararse. Por esta razón, aunque no esté explícito en el código, dropDuplicates() puede generar una operación WIDE.
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
FORMATOS_FECHA = ["dd/MM/yyyy", "MM/dd/yyyy", "yyyy/MM/dd", "yyyy-MM-dd", "dd-MM-yyyy"]

# TODO: usa F.coalesce(...) combinando un F.to_date(F.col("fecha"), fmt)
# por cada formato de FORMATOS_FECHA, y guarda el resultado en una
# columna nueva llamada EXACTAMENTE "fecha_parsed" (withColumn).
#
# Clasificación: El coalesce es una transformación fila a fila para reducir el numero de particiones, aunque internamente Spark tenga que evaluar varias expresiones para cada fila, no hay groupBy ni join, así que se considera NARROW. 
# Los formatos dd/MM/yyyy y MM/dd/yyyy pueden ser ambiguos cuando tanto el día como el mes tienen valores <= 12. En estos casos, el orden definido en FORMATOS_FECHA determina qué interpretación se utiliza (por lo tanto no siempre es posible determinar la fecha correcta únicamente a partir del valor original).

df_fechas = df_dedup.withColumn(
    "fecha_parsed",
    F.coalesce(
        F.try_to_date(F.col("fecha"), "dd/MM/yyyy"),
        F.try_to_date(F.col("fecha"), "MM/dd/yyyy"),
        F.try_to_date(F.col("fecha"), "yyyy/MM/dd"),
        F.try_to_date(F.col("fecha"), "yyyy-MM-dd"),
        F.try_to_date(F.col("fecha"), "dd-MM-yyyy"),
    )
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
    "Bogota": "BOGOTÁ",
    "bogota": "BOGOTÁ",
    "Bta": "BOGOTÁ",
    "BOGOTA": "BOGOTÁ",
    "BOGOTÁ": "BOGOTÁ",
    " Bogotá": "BOGOTÁ",
    "MDE": "MEDELLÍN",         # ejemplo: abreviatura de Medellín
    "Medellín": "MEDELLÍN",
    "MEDELLÍN": "MEDELLÍN",
    "medellin": "MEDELLÍN",
    "Medellin": "MEDELLÍN",
    "medellín": "MEDELLÍN",
    "CLO": "CALI",             # ejemplo: abreviatura de Cali (código de aeropuerto)
    "Cali": "CALI",
    " Cali": "CALI",
    "cali": "CALI",
    "cali": "CALI",
    "CALI": "CALI",
    "BAQ": "BARRANQUILLA",     # ejemplo: abreviatura de Barranquilla (código de aeropuerto)
    "BARRANQUILLA": "BARRANQUILLA",
    "Bquilla": "BARRANQUILLA",
    "Barranquilla": "BARRANQUILLA",
    "BAQ": "BARRANQUILLA",
    "barranquilla": "BARRANQUILLA",
    "BGA": "BUCARAMANGA",      # ejemplo: abreviatura de Bucaramanga (código de aeropuerto)
    "Bucaramanga": "BUCARAMANGA",
    "Buca": "BUCARAMANGA",
    "bucaramanga": "BUCARAMANGA",
    "BUCARAMANGA": "BUCARAMANGA",
    "Desconocido": "OTRO",
    "N/A": "OTRO",
    "NA": "OTRO",
    "otro": "OTRO",
    "OTRO": "OTRO"
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
# Clasificación: Aunque el mapa contiene múltiples condiciones, cada fila se procesa de manera independiente y no es necesario comparar una fila con otras filas ni redistribuir datos entre particiones. Por lo tanto, la operación es NARROW.

df_region = df_fechas.withColumn(
    "region_silver",
    construir_mapa(F.col("region"), MAPA_REGION, "OTRO")
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
MAPA_CANAL = {
    "APP_MOVIL": "app_movil",
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
# Clasificación: NARROW; La normalización de canal utiliza nuevamente construir_mapa() y aplica las transformaciones fila por fila mediante expresiones when(). Cada fila puede determinar su valor únicamente con el valor de su propia columna.

df_canal = df_region.withColumn(
    "canal_silver",
    construir_mapa(F.col("canal"), MAPA_CANAL, "otro_canal")
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

df_cast = df_canal \
    .withColumn("cantidad_num", F.col("cantidad").cast("double")) \
    .withColumn("precio_num", F.col("precio_unit").cast("double"))


# TODO paso 2: filtra para quedarte solo con las filas donde
# cantidad_num > 0 AND precio_num > 0 (ambos deben existir con valor
# válido para que el recálculo tenga sentido de negocio).
df_validado = df_cast.filter((F.col("cantidad_num") > 0) & (F.col("precio_num") > 0))

# TODO paso 3: agrega la columna "total_silver" =
# round(cantidad_num * precio_num, 2).
#
# Clasificación de los 3 pasos de arriba: Los tres pasos de esta sección son transformaciones NARROW (convertir cantidad y precio_unit a double, filtrar las filas donde ambos valores sean mayores que cero, calcular total_silver) ya que se realizan de manera independiente para cada fila, sin necesidad de comparar registros entre sí ni redistribuir los datos entre particiones mediante un shuffle.

df_total = df_validado.withColumn(
    "total_silver",
    F.round(F.col("cantidad_num") * F.col("precio_num"), 2)
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
# Clasificación: → NARROW; La extracción de la parte numérica de vendedor_id mediante regexp_extract() se realiza independientemente para cada fila. La expresión regular busca los caracteres numéricos dentro del valor original y devuelve únicamente esa parte (no es necesario comparar filas entre sí ni redistribuir los datos entre particiones)
df_vendedor = df_total.withColumn(
    "vendedor_id",
    F.regexp_extract(F.col("vendedor_id"), r"(\d+)", 1)
)

# TODO: valida "email_cliente" con una expresión regular de email
# razonable (usuario@dominio.tld) usando F.rlike(). Crea una columna
# booleana nueva llamada "email_valido". NO elimines ni pongas en null
# los emails inválidos -- solo márcalos.
#
# Clasificación: → NARROW; La validación de emails mediante rlike() se realiza independientemente para cada fila. Se aplica una expresión regular a cada valor de la columna email_cliente y devuelve un booleano (no es necesario comparar filas entre sí ni redistribuir los datos entre particiones).
df_tipos = df_vendedor.withColumn(
    "email_valido",
    F.col("email_cliente").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
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
# Clasificación: → WIDE porque Spark necesita comparar los registros nuevos (df_silver) con los registros existentes en la tabla Delta utilizando la clave de negocio pedido_id; Para determinar si cada registro corresponde a un registro existente (UPDATE) o a uno nuevo (INSERT), Spark necesita realizar una operación equivalente a un join entre ambas fuentes. Esto puede requerir redistribuir los datos entre particiones mediante shuffle.

if DeltaTable.isDeltaTable(spark, SILVER):
    print("3.7 Tabla Silver existe -- ejecutando MERGE")
    silver_table = DeltaTable.forPath(spark, SILVER)
    silver_table.alias("s").merge(
        df_silver.alias("n"),
        "s.pedido_id = n.pedido_id"
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

# exportar_muestra_csv.py — corre esto en EMR después de 02_silver.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ST1630-ExportMuestra").getOrCreate()

BUCKET = "st1630-nvcardozaa-2026"
SILVER = f"s3a://{BUCKET}/silver/pedidos"
CSV_10K = f"s3a://{BUCKET}/benchmark/csv_10k/"

df_silver = spark.read.format("delta").load(SILVER)
df_silver.select(
    "pedido_id", "fecha", "region", "canal", "categoria", "producto",
    "cantidad", "precio_unit", "total_silver"
).limit(10000).coalesce(1).write.mode("overwrite").option("header", "true").csv(CSV_10K)

print("Muestra de 10,000 filas exportada a:", CSV_10K)
spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1
