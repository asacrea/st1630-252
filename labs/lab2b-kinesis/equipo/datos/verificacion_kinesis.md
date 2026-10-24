# Verificación de la Parte 4 — Kinesis (sobre LocalStack)

**Fecha:** 2026-09-13 (20:40–21:02 hora Colombia = 01:40–02:02 UTC del 2026-09-14)
**Configuración del pipeline:** la misma de la Parte 3 (ventana de 5 minutos, watermark de 10
minutos, trigger cada 30 s, `outputMode("update")`, `MERGE` por
`(window_start, window_end, region)`). Solo cambia la fuente: `STREAM_SOURCE=kinesis`.

Las horas de ventanas, logs y Delta están en **UTC** (reloj de los contenedores).

## Por qué LocalStack y no AWS Academy

Las credenciales del Learner Lab funcionan para leer Kinesis, pero no para crear streams:

```
$ aws kinesis list-streams --region us-east-1
{ "StreamNames": [], "StreamSummaries": [] }

$ aws kinesis create-stream --stream-name pedidos-ventas-kinesis --shard-count 2 --region us-east-1
An error occurred (AccessDeniedException) when calling the CreateStream operation:
User: arn:aws:sts::040343073329:assumed-role/voclabs/user5062485=Juan_Jose_Diaz is not authorized
to perform: kinesis:CreateStream on resource: arn:aws:kinesis:us-east-1:040343073329:stream/pedidos-ventas-kinesis
because no identity-based policy allows the kinesis:CreateStream action
```

El mismo error sale en `us-west-2`: el rol `voclabs` no incluye `kinesis:CreateStream` en
ninguna región (Troubleshooting #6 del README). No se creó ningún recurso en AWS.

Para hacer la parte con experiencia real usamos **LocalStack 3.8**, que emula la API de
Kinesis en Docker. Todo lo de abajo (productor `boto3`, conector de Spark, checkpoint, `MERGE`)
habla con esa API igual que hablaría con AWS; lo que no existe es la consola, así que en lugar
de la captura del Data Viewer dejamos la salida de `get-records` (más abajo).

## Conector de Spark

`crear_stream_kinesis()` usa `format("aws-kinesis")` y opciones `kinesis.*`. Eso corresponde al
conector oficial de **AWS Labs** (`spark-sql-kinesis-connector`), no al paquete de Qubole que
cita el README (`com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0`), que es para Spark 3.0
y registra el formato `kinesis`.

El de AWS Labs no está en Maven Central. Se descargó desde el bucket público de AWS Labs:

| | |
|---|---|
| Archivo | `spark-streaming-sql-kinesis-connector_2.12-1.4.2.jar` |
| Origen | `https://awslabs-code-us-east-1.s3.amazonaws.com/spark-sql-kinesis-connector/` |
| Tamaño | 81,0 MB |
| SHA-256 | `30e734ae790324c8280129908fa95d9c180aaa5421bbe7863ad605cbfe5dae19` |
| Proveedor registrado | `org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider` |

Se guarda en el volumen Docker `lab2b-jars` (no en el repo) y el pipeline lo carga con
`spark.jars` cuando `STREAM_SOURCE=kinesis`.

### Rareza del conector con endpoints propios

Primer intento con `KINESIS_ENDPOINT=http://localstack:4566`:

```
java.lang.IllegalArgumentException: Invalid endpoint url received. Cannot parse region: http://localstack:4566
    at org.apache.spark.sql.connector.kinesis.package$.getRegionNameByEndpoint(package.scala:89)
    at org.apache.spark.sql.connector.kinesis.KinesisOptions$.apply(KinesisOptions.scala:330)
```

Cuando hay `kinesis.endpointUrl`, el conector intenta sacar la región del nombre del host aunque
ya se le haya pasado `kinesis.region`. Lo comprobamos llamando la función del conector desde
una JVM aparte:

| Endpoint | `getRegionNameByEndpoint` |
|---|---|
| `http://localstack:4566` | error `Cannot parse region` |
| `http://kinesis.us-east-1.localstack:4566` | `us-east-1` |
| `https://kinesis.us-east-1.amazonaws.com` | `us-east-1` |

En esa misma prueba confirmamos las llaves de opciones: `kinesis.region`,
`kinesis.endpointUrl`, `kinesis.streamName`, `kinesis.consumerType`, `kinesis.startingPosition`.

Solución sin tocar código: darle a LocalStack el alias de red `kinesis.us-east-1.localstack`.
Contra AWS real no hace falta, porque el host oficial ya tiene esa forma.

## Cómo se corrió

Desde `labs/lab2b-kinesis/`, en la red `lab2a-kafka_default`:

```bash
# 1. LocalStack y el stream (2 shards)
docker run -d --name localstack --network lab2a-kafka_default -p 4566:4566 -e SERVICES=kinesis localstack/localstack:3.8
docker exec localstack awslocal kinesis create-stream --stream-name pedidos-ventas-kinesis --shard-count 2
docker exec localstack awslocal kinesis describe-stream-summary --stream-name pedidos-ventas-kinesis \
  --query "StreamDescriptionSummary.StreamStatus"          # "ACTIVE"

# 2. Alias de red para la rareza del conector
docker network disconnect lab2a-kafka_default localstack
docker network connect --alias kinesis.us-east-1.localstack lab2a-kafka_default localstack

# 3. Productor (2 corridas). boto3 toma el endpoint de AWS_ENDPOINT_URL; credenciales ficticias
docker run --rm --network lab2a-kafka_default -v "${PWD}/equipo/scripts:/scripts" \
  -e AWS_ENDPOINT_URL=http://localstack:4566 -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 st1630-lab2a-spark \
  sh -c "pip install -q boto3 && python -u /scripts/kinesis_producer.py"

# 4. Pipeline con fuente Kinesis, tabla y checkpoint separados de los de Kafka
docker run -d --name lab2b-kinesis-stream --network lab2a-kafka_default \
  -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake -v lab2a-ivy:/root/.ivy2 -v lab2b-jars:/jars \
  -e STREAM_SOURCE=kinesis -e KINESIS_ENDPOINT=http://kinesis.us-east-1.localstack:4566 \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_REGION=us-east-1 \
  -e SILVER_STREAMING_PATH=/tmp/lake/silver/ventas_streaming_kinesis \
  st1630-lab2a-spark python -u /scripts/streaming_pipeline.py

# 5. Lectura de la tabla
docker run --rm -e SILVER_STREAMING_PATH=/tmp/lake/silver/ventas_streaming_kinesis \
  -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake -v lab2a-ivy:/root/.ivy2 \
  st1630-lab2a-spark python /scripts/leer_ventanas.py "kinesis tras corrida 2"
```

Checkpoint: `/tmp/lake/checkpoints/lab2b-kinesis` (por defecto según `STREAM_SOURCE`).

## Stream y reparto por shard

Stream `pedidos-ventas-kinesis`: `ACTIVE`, 2 shards abiertos, retención de 24 horas.

| Shard | `StartingHashKey` |
|---|---|
| `shardId-000000000000` | 0 |
| `shardId-000000000001` | 170141183460469231731687303715884105727 (2^127 − 1) |

Kinesis asigna el shard con el MD5 de la `PartitionKey` (128 bits) comparado contra esos rangos.
Con `PartitionKey=region`, calculado de antemano:

| Región | Peso en `kinesis_producer.py` | Shard | Corrida 1 | Corrida 2 |
|---|---|---|---|---|
| Bogotá | 40 % | 1 | 197 | 198 |
| Medellín | 20 % | **0** | 103 | 95 |
| Cali | 15 % | 1 | 77 | 80 |
| Otro | 15 % | 1 | 80 | 75 |
| Barranquilla | 10 % | 1 | 43 | 52 |
| **Shard 0 / Shard 1** | | | **103 / 397** | **95 / 405** |

Cerca del 80 % de los pedidos cae en el shard 1: el mismo problema de partición caliente del
Lab 2a (P0 con el 55,6 %), por la misma decisión de particionar por región. La salida del
productor lo refleja: en la corrida 1, 9 de las 10 muestras impresas decían
`shard=shardId-000000000001`.

El productor de Kinesis manda un esquema reducido (`pedido_id`, `region`, `categoria`, `total`,
`timestamp`, `devuelto`) y no genera Bucaramanga. Con `PEDIDO_SCHEMA`, los campos que faltan
quedan en `null`; la ventana solo usa `region`, `total` y `pedido_id`, así que no afecta.

## Log del pipeline

```
01:57:40  [batch 0] 5 filas de ventana actualizadas   <- corrida 1 (ya estaba en el stream), desde TRIM_HORIZON
01:57:55  [batch 1] 0 filas de ventana actualizadas   <- batch sin datos para avanzar el watermark
02:01:10  [batch 2] 5 filas de ventana actualizadas   <- corrida 2
02:01:27  [batch 3] 0 filas de ventana actualizadas
```

La corrida 1 se publicó antes de arrancar el pipeline (hacia las 01:42) y entró completa en el
batch 0. La corrida 2 también entró completa en un solo batch.

## Checkpoint: de `TRIM_HORIZON` a `AFTER_SEQUENCE_NUMBER`

`offsets/0` (inicio del batch 0):

```json
{"metadata":{"streamName":"pedidos-ventas-kinesis","batchId":"0"},
 "shardId-000000000001":{"subSequenceNumber":"-1","isLast":"true","iteratorType":"TRIM_HORIZON","iteratorPosition":""},
 "shardId-000000000000":{"subSequenceNumber":"-1","isLast":"true","iteratorType":"TRIM_HORIZON","iteratorPosition":""}}
```

`offsets/2` (inicio del batch 2, después de leer la corrida 1):

```json
{"metadata":{"streamName":"pedidos-ventas-kinesis","batchId":"1"},
 "shardId-000000000001":{"subSequenceNumber":"-1","isLast":"true","iteratorType":"AFTER_SEQUENCE_NUMBER",
   "iteratorPosition":"49678318705339624228487027919589346238780577666306146322"},
 "shardId-000000000000":{"subSequenceNumber":"-1","isLast":"true","iteratorType":"AFTER_SEQUENCE_NUMBER",
   "iteratorPosition":"49678318705317323483288497296092386329541228327436550146"}}
```

En Kafka el checkpoint guardaba un número por partición (`{"0":1678, ...}`, "el siguiente offset
por leer"). Aquí guarda, por shard, un tipo de iterador y un `SequenceNumber` ("continuar
después de esta secuencia"). El batch 2 leyó solo los registros posteriores a esas dos
secuencias: la corrida 2, sin volver a leer la 1.

## Tabla Delta `ventas_streaming_kinesis`

Salida de `leer_ventanas.py "kinesis tras corrida 2"`:

```
filas_totales             = 10
suma_num_pedidos          = 1000
duplicados_ventana_region = 0

+-------------------+-------------------+------------+--------------------+-----------+
|window_start       |window_end         |region      |ventas_totales      |num_pedidos|
+-------------------+-------------------+------------+--------------------+-----------+
|2026-09-14 01:40:00|2026-09-14 01:45:00|Barranquilla|5.422058786000001E7 |43         |
|2026-09-14 01:40:00|2026-09-14 01:45:00|Bogotá      |2.4129000103000006E8|197        |
|2026-09-14 01:40:00|2026-09-14 01:45:00|Cali        |9.981636963000001E7 |77         |
|2026-09-14 01:40:00|2026-09-14 01:45:00|Medellín    |1.4060322933999997E8|103        |
|2026-09-14 01:40:00|2026-09-14 01:45:00|Otro        |9.400188263E7       |80         |
|2026-09-14 01:55:00|2026-09-14 02:00:00|Barranquilla|6.874067453E7       |52         |
|2026-09-14 01:55:00|2026-09-14 02:00:00|Bogotá      |2.4213203167000014E8|198        |
|2026-09-14 01:55:00|2026-09-14 02:00:00|Cali        |1.0322278014E8      |80         |
|2026-09-14 01:55:00|2026-09-14 02:00:00|Medellín    |1.1023511204000002E8|95         |
|2026-09-14 01:55:00|2026-09-14 02:00:00|Otro        |9.223727211999996E7 |75         |
+-------------------+-------------------+------------+--------------------+-----------+

+-------+-----------------------+---------+
|version|timestamp              |operation|
+-------+-----------------------+---------+
|0      |2026-09-14 01:57:50.191|WRITE    |
|1      |2026-09-14 02:01:24.182|MERGE    |
+-------+-----------------------+---------+
```

Las 5 filas de la ventana 01:40–01:45 no cambiaron con la corrida 2, que creó 5 filas nuevas en
01:55–02:00. Mismo comportamiento que con Kafka, con `aplicar_ventana()` y `escribir_batch()`
sin ningún cambio.

## Registros en el stream (en lugar del Data Viewer)

`get-records` sobre el shard 0 desde `TRIM_HORIZON`, 2 registros, con `Data` decodificado de
base64:

```bash
it=$(docker exec localstack awslocal kinesis get-shard-iterator --stream-name pedidos-ventas-kinesis \
  --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --query ShardIterator --output text)
docker exec localstack awslocal kinesis get-records --shard-iterator "$it" --limit 2
```

```json
{
  "SequenceNumber": "49678318705317323483288497295969075895940532234606346242",
  "PartitionKey": "Medellín",
  "ApproximateArrivalTimestamp": 1789350136.958,
  "Data (decodificado)": {
    "pedido_id": "e49134c6-c836-48e5-a028-d87041e6935a",
    "region": "Medellín",
    "categoria": "Alimentos",
    "total": 1317503.25,
    "timestamp": "2026-09-14T01:42:26.241494Z",
    "devuelto": false
  }
}
{
  "SequenceNumber": "49678318705317323483288497295970284821760146932500529154",
  "PartitionKey": "Medellín",
  "ApproximateArrivalTimestamp": 1789350137.672,
  "Data (decodificado)": {
    "pedido_id": "f18603ea-a454-4cac-8f69-0094efd13ba5",
    "region": "Medellín",
    "categoria": "Ropa",
    "total": 2249604.79,
    "timestamp": "2026-09-14T01:42:26.959022Z",
    "devuelto": false
  }
}
MillisBehindLatest: 1046419
```

- Todo el shard 0 es `PartitionKey: "Medellín"`, como anticipaba el hash.
- `ApproximateArrivalTimestamp` (1789350136.958 = 01:42:16 UTC) es lo que el pipeline usa como
  `kafka_time`. A diferencia del CreateTime de Kafka, que fija el productor, este lo fija el
  servicio al recibir el registro. Aquí quedó ~10 s *antes* del `timestamp` que escribió el
  productor (01:42:26). No lo investigamos a fondo; lo más probable es un desfase de reloj de la
  emulación de LocalStack, y sirve como ejemplo de que el tiempo de evento depende de quién
  pone la marca.
- `MillisBehindLatest` ≈ 1.046.000 ms (~17 min): este iterador empezó en `TRIM_HORIZON` y está
  17 minutos detrás del registro más reciente. Es el mismo concepto que
  `GetRecords.IteratorAgeMilliseconds` en CloudWatch y que el lag de un consumer group en Kafka.
