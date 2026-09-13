"""01_bronze.py — Lab 1b (ST1630-2026-2, S5-S6)

Ingesta cruda a Bronze: el CSV entra a Delta Lake TAL CUAL viene, sin
limpiar ni tipar nada. Bronze es la capa de "verdad del origen" -- si
algo sale mal en Silver o Gold, siempre puedes volver a Bronze y
reprocesar, porque Bronze nunca se sobreescribe con datos transformados.

Este script tiene bloques marcados con # TODO -- ESE es tu trabajo.
Todo lo demás (imports, rutas, la verificación del final) ya está
resuelto para que puedas concentrarte en las partes que de verdad
enseñan algo nuevo esta semana.

Uso:
    spark-submit 01_bronze.py

Qué puedes delegar: dudas de sintaxis puntuales si te trabas en un
TODO (¿cómo se llama el método para X?). Qué NO puedes delegar: por
qué el schema es 100% string -- tienes que poder explicarlo con tus
propias palabras (te lo preguntamos en la defensa del lab).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("ST1630-Lab1b-Bronze").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "32")  # clúster del curso: 4 executors x 8 cores

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
BUCKET = "st1630-dagutierrl-2026"  # EDITAR: el mismo bucket del Lab 1a
RAW = f"s3a://{BUCKET}/raw/ventas_colombia_raw.csv"
BRONZE = f"s3a://{BUCKET}/bronze/pedidos"
# ─────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════
# TODO 1 · Schema explícito -- TODOS los campos como StringType
# ═══════════════════════════════════════════════════════════════
# Justificación (esto SÍ te lo damos resuelto -- entiéndelo antes de
# construir el schema): Bronze recibe el dato tal cual el sistema de
# origen lo entregó, sin asumir ningún tipo. Si aquí ya castearas
# 'total' a double, por ejemplo, Spark tendría que decidir qué hacer
# con un valor corrupto como una celda vacía o con letras -- y esa
# decisión (¿null? ¿0? ¿falla el job?) es una transformación, no una
# ingesta. La conversión de tipos es responsabilidad exclusiva de
# Silver, donde SÍ hay contexto de negocio para decidir cómo tratar
# cada caso raro.
#
# Las columnas del CSV crudo (mismo orden que ../datos/ventas_colombia_raw.csv):
#   pedido_id, fecha, region, canal, categoria, producto, cantidad,
#   precio_unit, total, vendedor_id, email_cliente, metodo_pago,
#   devuelto, calificacion
#
# TODO: construye BRONZE_SCHEMA como un StructType con un
# StructField(nombre, StringType(), True) por cada una de las 14
# columnas de arriba, en ese mismo orden.
BRONZE_SCHEMA = StructType([
    StructField("pedido_id", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("producto", StringType(), True),
    StructField("cantidad", StringType(), True),
    StructField("precio_unit", StringType(), True),
    StructField("total", StringType(), True),
    StructField("email_cliente", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("devuelto", StringType(), True),
    StructField("calificacion", StringType(), True),
    StructField("region", StringType(), True),
    StructField("canal", StringType(), True),
    StructField("vendedor_id", StringType(), True)
])

# ═══════════════════════════════════════════════════════════════
# TODO 2 · Lectura del CSV con schema explícito
# ═══════════════════════════════════════════════════════════════
# TODO: usa spark.read, con .option("header", "true"), .schema(BRONZE_SCHEMA)
# y .csv(RAW) para leer el archivo crudo en df_raw.
#
# Clasificación: → [NARROW ✅ / WIDE ❌] -- justifica en un comentario
# por qué (pista: ¿esta lectura necesita comparar o mover datos entre
# particiones para poder aplicarle el schema?).
df_raw = (
    spark.read
    .option("header", "true")
    .schema(BRONZE_SCHEMA)
    .csv(RAW)
)

# NARROW 
# Leer el CSV y aplicar el schema no requiere comparar ni redistribuir
# registros entre particiones. Cada partición puede leer y convertir
# sus propias filas de acuerdo con el schema.

# ═══════════════════════════════════════════════════════════════
# TODO 3 · Columnas de auditoría
# ═══════════════════════════════════════════════════════════════
# TODO: a partir de df_raw, agrega dos columnas con withColumn():
#   - "_ingested_at": el timestamp de cuándo se corrió esta ingesta
#     (busca la función de pyspark.sql.functions que da la hora actual)
#   - "_source_file": de qué archivo físico vino cada fila
#     (busca la función que expone el nombre del archivo de origen)
#
# Clasificación: → [NARROW ✅ / WIDE ❌] -- justifica.
df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

# NARROW 
# withColumn() agrega columnas calculadas a cada fila sin necesidad
# de comparar o redistribuir datos entre particiones. Por eso no genera
# un shuffle.

# ═══════════════════════════════════════════════════════════════
# TODO 4 · Escritura a Delta en modo append
# ═══════════════════════════════════════════════════════════════
# TODO: escribe df_bronze a la ruta BRONZE en formato "delta", modo
# "append" (NO "overwrite" -- Bronze acumula, nunca reemplaza).
#
# Clasificación: → [NARROW ✅ / WIDE ❌] -- justifica, y compara mentalmente
# contra el MERGE que vas a escribir en 02_silver.py (Parte 3.7): ¿por
# qué ESTA escritura no necesita comparar contra lo que ya existe en
# la tabla, y esa sí?
(
    df_bronze
    .write
    .format("delta")
    .mode("append")
    .save(BRONZE)
)

# NARROW 
# La escritura en modo append simplemente agrega los nuevos datos a
# la tabla Delta. No necesita comparar las filas nuevas contra las
# existentes ni redistribuirlas para encontrar coincidencias.
#
# En cambio, un MERGE en Silver sí necesita buscar coincidencias entre
# los registros nuevos y los existentes, por lo que puede requerir
# shuffle/exchange.

print(f"Bronze escrito en: {BRONZE}")

# ═══════════════════════════════════════════════════════════════
# Verificación: los problemas del raw SÍ deben estar en Bronze
# (esta parte ya está resuelta -- es tu "examen" automático de que los
# TODOs de arriba quedaron bien).
# ═══════════════════════════════════════════════════════════════
# Si Bronze estuviera limpio en este punto, algo se transformó de más
# -- eso sería un error de diseño, no una mejora.
df_check = spark.read.format("delta").load(BRONZE)

n_total = df_check.count()

n_pedido_null = df_check.filter(
    F.col("pedido_id").isNull()
).count()

n_total_null = df_check.filter(
    F.col("total").isNull() | (F.col("total") == "")
).count()

n_dup = n_total - df_check.dropDuplicates(
    BRONZE_SCHEMA.fieldNames()
).count()

print(f"\n=== Verificación Bronze (deben aparecer los problemas del raw) ===")
print(f"Filas totales: {n_total:,}")
print(f"pedido_id nulos: {n_pedido_null:,} (debe ser > 0)")
print(f"total nulos/vacíos: {n_total_null:,} (debe ser > 0)")
print(f"Duplicados exactos: {n_dup:,} (debe ser > 0)")

assert n_pedido_null > 0 and n_total_null > 0 and n_dup > 0, (
    "Bronze no debería estar limpio -- revisa tus TODOs, probablemente "
)
# WIDE ❌ Exchange: dropDuplicates() sobre TODAS las columnas necesita
# que Spark calcule un hash de la fila completa y reparticione por ese
# hash, para que dos filas idénticas -- que pudieron haber llegado en
# particiones distintas del archivo original -- terminen comparándose
# en el mismo executor. Eso es un shuffle real (shuffle write local +
# shuffle read por red), con su propio nodo Exchange en el plan físico
# -- lo vas a confirmar tú mismo en Spark UI en la Parte 3.8 del lab.
n_dup = n_total - df_check.dropDuplicates(BRONZE_SCHEMA.fieldNames()).count()

print(f"\n=== Verificación Bronze (deben aparecer los problemas del raw) ===")
print(f"Filas totales: {n_total:,}")
print(f"pedido_id nulos: {n_pedido_null:,} (debe ser > 0)")
print(f"total nulos: {n_total_null:,} (debe ser > 0)")
print(f"Duplicados exactos: {n_dup:,} (debe ser > 0)")
assert n_pedido_null > 0 and n_total_null > 0 and n_dup > 0, (
    "Bronze no debería estar limpio -- revisa tus TODOs, probablemente "
    "se te coló un filtro o un cast antes de escribir."
)

# ── Inspeccionar el _delta_log ──────────────────────────────────
# Cada escritura a una tabla Delta genera un archivo JSON de commit en
# <ruta_tabla>/_delta_log/00000000000000000000.json (el primero),
# 00000000000000000001.json (el segundo), etc. Ese JSON es la fuente
# de verdad de Delta Lake: no es un índice derivado, ES la definición
# de qué archivos Parquet componen la tabla en cada versión.
print(f"""
=== Cómo inspeccionar el primer commit de _delta_log ===
aws s3 cp {BRONZE}/_delta_log/00000000000000000000.json - | python3 -m json.tool | head -50

Busca estas líneas dentro del JSON y anota en tu bitácora qué
representa cada una:
  - "commitInfo"  -> ¿quién hizo el commit? ¿cuándo? ¿con qué operación?
  - "metaData"    -> ¿coincide el schema con tu BRONZE_SCHEMA + las 2
                     columnas de auditoría del TODO 3?
  - "add"         -> ¿cuántos archivos Parquet agregó este commit?
""")

spark.stop()

# ### Cuando termines: no olvides apagar el clúster EMR si ya no lo
# ### vas a usar en las próximas horas:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1
"""

spark-submit   --conf spark.jars=/usr/share/aws/delta/lib/delta-core.jar,/usr/share/aws/delta/lib/delta-storage.jar   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog   01_bronze.py


Lo que me arrojo el json:

[hadoop@ip-10-0-1-54 ~]$ while IFS= read -r line; do
>     echo "$line" | python3 -m json.tool
> done < /tmp/commit.json
{
    "commitInfo": {
        "timestamp": 1787193544283,
        "operation": "WRITE",
        "operationParameters": {
            "mode": "Append",
            "partitionBy": "[]"
        },
        "isolationLevel": "Serializable",
        "isBlindAppend": true,
        "operationMetrics": {
            "numFiles": "4",
            "numOutputRows": "101500",
            "numOutputBytes": "3297714"
        },
        "engineInfo": "Apache-Spark/3.4.1-amzn-2 Delta-Lake/2.4.0",
        "txnId": "b227ec02-019b-42c9-bcd6-3b3ddb3d2aba"
    }
}
{
    "protocol": {
        "minReaderVersion": 1,
        "minWriterVersion": 2
    }
}
{
    "metaData": {
        "id": "baa1d5ec-da40-4e0a-92a7-4b362bf585c7",
        "format": {
            "provider": "parquet",
            "options": {}
        },
        "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"pedido_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fecha\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"categoria\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"producto\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cantidad\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"precio_unit\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"total\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"email_cliente\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"metodo_pago\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"devuelto\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"calificacion\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"canal\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"vendedor_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_ingested_at\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"_source_file\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
        "partitionColumns": [],
        "configuration": {},
        "createdTime": 1787193532304
    }
}
{
    "add": {
        "path": "part-00000-6ef25db4-3d4c-4ea1-8cc2-f44c3d505897-c000.snappy.parquet",
        "partitionValues": {},
        "size": 1078337,
        "modificationTime": 1787193543000,
        "dataChange": true,
        "stats": "{\"numRecords\":33431,\"minValues\":{\"pedido_id\":\"PED-000001\",\"fecha\":\"01-01-2025\",\"categoria\":\"Alimentos\",\"producto\":\"Aceite\",\"cantidad\":\"-1.0\",\"precio_unit\":\"-101500.0\",\"total\":\"-10084.394253890097\",\"email_cliente\":\"andres.castro100@yahoo.com\",\"metodo_pago\":\"PSE\",\"devuelto\":\"False\",\"calificacion\":\"1\",\"region\":\" Bogot\u00e1\",\"canal\":\"APP MOVIL\",\"vendedor_id\":\"1000\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\"},\"maxValues\":{\"pedido_id\":\"PED-099998\",\"fecha\":\"31/12/2025\",\"categoria\":\"Ropa\",\"producto\":\"Zapatos\",\"cantidad\":\"5.0\",\"precio_unit\":\"99900.0\",\"total\":\"999900.0\",\"email_cliente\":\"valentina.torres995@yahoo.com\",\"metodo_pago\":\"tarjeta_debito\",\"devuelto\":\"True\",\"calificacion\":\"5\",\"region\":\"otro\",\"canal\":\"tienda\",\"vendedor_id\":\"v9998\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\ufffd\"},\"nullCount\":{\"pedido_id\":75,\"fecha\":0,\"categoria\":0,\"producto\":0,\"cantidad\":0,\"precio_unit\":0,\"total\":871,\"email_cliente\":54,\"metodo_pago\":0,\"devuelto\":0,\"calificacion\":0,\"region\":0,\"canal\":0,\"vendedor_id\":0,\"_ingested_at\":0,\"_source_file\":0}}"
    }
}
{
    "add": {
        "path": "part-00001-42270abb-5966-4b4f-9546-9bc9e3bd08f7-c000.snappy.parquet",
        "partitionValues": {},
        "size": 1081167,
        "modificationTime": 1787193543000,
        "dataChange": true,
        "stats": "{\"numRecords\":33442,\"minValues\":{\"pedido_id\":\"PED-000005\",\"fecha\":\"01-01-2025\",\"categoria\":\"Alimentos\",\"producto\":\"Aceite\",\"cantidad\":\"-1.0\",\"precio_unit\":\"-100200.0\",\"total\":\"-10087.659579673553\",\"email_cliente\":\"andres.castro108@outlook.com\",\"metodo_pago\":\"PSE\",\"devuelto\":\"False\",\"calificacion\":\"1\",\"region\":\" Bogot\u00e1\",\"canal\":\"APP MOVIL\",\"vendedor_id\":\"1000\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\"},\"maxValues\":{\"pedido_id\":\"PED-099999\",\"fecha\":\"31/12/2025\",\"categoria\":\"Ropa\",\"producto\":\"Zapatos\",\"cantidad\":\"5.0\",\"precio_unit\":\"99900.0\",\"total\":\"999600.0\",\"email_cliente\":\"valentina.torres99@hotmail.com\",\"metodo_pago\":\"tarjeta_debito\",\"devuelto\":\"True\",\"calificacion\":\"5\",\"region\":\"otro\",\"canal\":\"tienda\",\"vendedor_id\":\"v9993\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\ufffd\"},\"nullCount\":{\"pedido_id\":71,\"fecha\":0,\"categoria\":0,\"producto\":0,\"cantidad\":0,\"precio_unit\":0,\"total\":828,\"email_cliente\":55,\"metodo_pago\":0,\"devuelto\":0,\"calificacion\":0,\"region\":0,\"canal\":0,\"vendedor_id\":0,\"_ingested_at\":0,\"_source_file\":0}}"
    }
}
{
    "add": {
        "path": "part-00002-8b167461-dc2b-4e89-8b38-bc06ccc1aa86-c000.snappy.parquet",
        "partitionValues": {},
        "size": 1080165,
        "modificationTime": 1787193543000,
        "dataChange": true,
        "stats": "{\"numRecords\":33426,\"minValues\":{\"pedido_id\":\"PED-000002\",\"fecha\":\"01-01-2025\",\"categoria\":\"Alimentos\",\"producto\":\"Aceite\",\"cantidad\":\"-1.0\",\"precio_unit\":\"-101400.0\",\"total\":\"-10028.638317726201\",\"email_cliente\":\"andres.castro100@hotmail.com\",\"metodo_pago\":\"PSE\",\"devuelto\":\"False\",\"calificacion\":\"1\",\"region\":\" Bogot\u00e1\",\"canal\":\"APP MOVIL\",\"vendedor_id\":\"1000\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\"},\"maxValues\":{\"pedido_id\":\"PED-100000\",\"fecha\":\"31/12/2025\",\"categoria\":\"Ropa\",\"producto\":\"Zapatos\",\"cantidad\":\"5.0\",\"precio_unit\":\"99900.0\",\"total\":\"999900.0\",\"email_cliente\":\"valentina.torres996@yahoo.com\",\"metodo_pago\":\"tarjeta_debito\",\"devuelto\":\"True\",\"calificacion\":\"5\",\"region\":\"otro\",\"canal\":\"tienda\",\"vendedor_id\":\"v9994\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\ufffd\"},\"nullCount\":{\"pedido_id\":61,\"fecha\":0,\"categoria\":0,\"producto\":0,\"cantidad\":0,\"precio_unit\":0,\"total\":842,\"email_cliente\":33,\"metodo_pago\":0,\"devuelto\":0,\"calificacion\":0,\"region\":0,\"canal\":0,\"vendedor_id\":0,\"_ingested_at\":0,\"_source_file\":0}}"
    }
}
{
    "add": {
        "path": "part-00003-5fb6111e-a807-4778-942d-52e53fa55dde-c000.snappy.parquet",
        "partitionValues": {},
        "size": 58045,
        "modificationTime": 1787193542000,
        "dataChange": true,
        "stats": "{\"numRecords\":1201,\"minValues\":{\"pedido_id\":\"PED-000012\",\"fecha\":\"01-01-2025\",\"categoria\":\"Alimentos\",\"producto\":\"Aceite\",\"cantidad\":\"-1.0\",\"precio_unit\":\"-107200.0\",\"total\":\"-17910.71952947237\",\"email_cliente\":\"andres.castro118@gmail.com\",\"metodo_pago\":\"PSE\",\"devuelto\":\"False\",\"calificacion\":\"1\",\"region\":\" Bogot\u00e1\",\"canal\":\"APP MOVIL\",\"vendedor_id\":\"1021\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\"},\"maxValues\":{\"pedido_id\":\"PED-099932\",\"fecha\":\"31/12/2025\",\"categoria\":\"Ropa\",\"producto\":\"Zapatos\",\"cantidad\":\"5.0\",\"precio_unit\":\"99600.0\",\"total\":\"99200.0\",\"email_cliente\":\"valentina.torres995@gmail.com\",\"metodo_pago\":\"tarjeta_debito\",\"devuelto\":\"True\",\"calificacion\":\"5\",\"region\":\"otro\",\"canal\":\"tienda\",\"vendedor_id\":\"v9511\",\"_ingested_at\":\"2026-08-20T02:38:52.955Z\",\"_source_file\":\"s3a://st1630-ealvarezc1-2026/raw\ufffd\"},\"nullCount\":{\"pedido_id\":1,\"fecha\":0,\"categoria\":0,\"producto\":0,\"cantidad\":0,\"precio_unit\":0,\"total\":30,\"email_cliente\":2,\"metodo_pago\":0,\"devuelto\":0,\"calificacion\":0,\"region\":0,\"canal\":0,\"vendedor_id\":0,\"_ingested_at\":0,\"_source_file\":0}}"
    }
}
[hadoop@ip-10-0-1-54 ~]$
"""