# Lab 2b · Streaming con Spark Structured Streaming — entrega del equipo

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 2026-09-13
**Estudiantes:**
Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co · Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
Sebastián Durán Fernández — sduranf@eafit.edu.co · Daniel Arcila Salazar — darcilas1@eafit.edu.co

**Parte 4 (Kinesis, opcional): sí, sobre LocalStack.** AWS Academy no permite
`kinesis:CreateStream` en nuestro Learner Lab, así que emulamos Kinesis Data Streams en Docker.
Detalle en `datos/verificacion_kinesis.md`.

## Qué hay en esta carpeta

```
labs/lab2b-kinesis/equipo/
├── README.md                       # este archivo
├── streaming_design.md             # Preguntas 1-7
├── bitacora_delegacion.md
├── scripts/
│   ├── streaming_pipeline.py       # TODO Partes 1, 2 y 3 + ajustes para Kinesis/LocalStack
│   ├── kinesis_producer.py         # Parte 4, sin cambios respecto al del profesor
│   └── leer_ventanas.py            # utilidad propia de verificación
└── datos/
    ├── verificacion_sink.md        # Parte 3: 4 corridas, pruebas de reinicio, métricas de MERGE
    └── verificacion_kinesis.md     # Parte 4: LocalStack, conector, shards, get-records
```

No hay `datos/kinesis_data_viewer.png`: LocalStack no tiene consola web. En su lugar,
`verificacion_kinesis.md` trae la salida de `get-records` con los JSON de los pedidos.

## Configuración

| | |
|---|---|
| Kafka | La del Lab 2a: `confluentinc/cp-kafka:7.6.0` en KRaft, con `../lab2a-kafka/equipo/docker-compose.override.yml` |
| Topic | `pedidos-ventas`, 4 particiones, factor de replicación 1 |
| Spark / Delta | Imagen `st1630-lab2a-spark` del Lab 2a: PySpark 3.5.6, delta-spark 3.3.0, Java 17 |
| Conector Kafka | `spark-sql-kafka-0-10_2.12:3.5.6`, agregado por el script |
| Kinesis | LocalStack 3.8, stream `pedidos-ventas-kinesis` con 2 shards |
| Conector Kinesis | AWS Labs `spark-streaming-sql-kinesis-connector_2.12-1.4.2.jar` (81 MB, fuera de Maven) |
| Ventana / watermark | 5 minutos / 10 minutos sobre `kafka_time` |
| Trigger / outputMode | `processingTime="30 seconds"` / `update` |
| Sink | `foreachBatch` + `MERGE` Delta por `(window_start, window_end, region)` |
| Host | Windows 11, Docker Desktop |

Todo corre en contenedores sobre la red `lab2a-kafka_default`. Spark no corre nativo en
Windows (mismo motivo que en el Lab 2a).

## Resultados

| Métrica | Kafka | Kinesis (LocalStack) |
|---|---|---|
| Corridas del productor | 4 (1.000 pedidos cada una) | 2 (500 pedidos cada una) |
| Filas en Delta | 18 (3 ventanas × 6 regiones) | 10 (2 ventanas × 5 regiones) |
| Suma de `num_pedidos` | 4.000 | 1.000 |
| Duplicados ventana+región | 0 | 0 |
| Crecimiento de una ventana abierta | Bogotá 409 → 806 en la misma ventana | no se probó |
| Reinicio con checkpoint | no reprocesa; el batch 10 leyó solo los 1.000 nuevos | — |
| Reinicio sin checkpoint | relee los 4.000; `MERGE` con 18 updated / 0 inserted | — |
| Partición / shard caliente | P0 con Bogotá y Cali | shard 1 con el 79,4 % (todo menos Medellín) |

## Cómo correr esta entrega

Desde `labs/lab2b-kinesis/` salvo que se indique otra carpeta. Los comandos son para una sola
línea en PowerShell o bash; aquí se parten con `\` solo para leerlos.

### 1. Kafka y la imagen de Spark (desde `labs/lab2a-kafka/`)

```bash
docker compose -f docker-compose.yml -f equipo/docker-compose.override.yml up -d
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas --partitions 4 \
  --replication-factor 1 --bootstrap-server localhost:9092
docker build -f equipo/Dockerfile.spark -t st1630-lab2a-spark equipo/
```

### 2. Pipeline con fuente Kafka

```bash
docker run -d --name lab2b-stream --network lab2a-kafka_default \
  -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake -v lab2a-ivy:/root/.ivy2 \
  -e KAFKA_BOOTSTRAP=kafka:29092 st1630-lab2a-spark python -u /scripts/streaming_pipeline.py
docker logs -f lab2b-stream
```

### 3. Productor de Kafka (desde `labs/lab2a-kafka/`)

```bash
docker run --rm --network lab2a-kafka_default -v "${PWD}/equipo/scripts:/scripts" \
  -e KAFKA_BOOTSTRAP=kafka:29092 st1630-lab2a-spark python -u /scripts/productor_kafka.py
```

### 4. Verificar la tabla

```bash
docker run --rm -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake \
  -v lab2a-ivy:/root/.ivy2 st1630-lab2a-spark python /scripts/leer_ventanas.py "etiqueta"
```

Para la tabla de Kinesis, agregar `-e SILVER_STREAMING_PATH=/tmp/lake/silver/ventas_streaming_kinesis`.

### 5. Parte 4: Kinesis sobre LocalStack

```bash
# LocalStack y el stream
docker run -d --name localstack --network lab2a-kafka_default -p 4566:4566 -e SERVICES=kinesis localstack/localstack:3.8
docker exec localstack awslocal kinesis create-stream --stream-name pedidos-ventas-kinesis --shard-count 2

# Alias de red que exige el conector (ver "Desviaciones")
docker network disconnect lab2a-kafka_default localstack
docker network connect --alias kinesis.us-east-1.localstack lab2a-kafka_default localstack

# Conector de AWS Labs en un volumen (una sola vez)
docker run --rm -v lab2b-jars:/jars st1630-lab2a-spark python -c "import urllib.request; urllib.request.urlretrieve('https://awslabs-code-us-east-1.s3.amazonaws.com/spark-sql-kinesis-connector/spark-streaming-sql-kinesis-connector_2.12-1.4.2.jar', '/jars/spark-streaming-sql-kinesis-connector_2.12-1.4.2.jar')"

# Productor (credenciales ficticias: LocalStack acepta cualquiera)
docker run --rm --network lab2a-kafka_default -v "${PWD}/equipo/scripts:/scripts" \
  -e AWS_ENDPOINT_URL=http://localstack:4566 -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 st1630-lab2a-spark \
  sh -c "pip install -q boto3 && python -u /scripts/kinesis_producer.py"

# Pipeline con fuente Kinesis (tabla y checkpoint separados de los de Kafka)
docker run -d --name lab2b-kinesis-stream --network lab2a-kafka_default \
  -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake -v lab2a-ivy:/root/.ivy2 -v lab2b-jars:/jars \
  -e STREAM_SOURCE=kinesis -e KINESIS_ENDPOINT=http://kinesis.us-east-1.localstack:4566 \
  -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_REGION=us-east-1 \
  -e SILVER_STREAMING_PATH=/tmp/lake/silver/ventas_streaming_kinesis \
  st1630-lab2a-spark python -u /scripts/streaming_pipeline.py
```

Contra AWS real bastaría con no pasar `KINESIS_ENDPOINT` ni las credenciales ficticias, y
montar credenciales válidas.

### 6. Limpieza

```bash
docker stop lab2b-stream lab2b-kinesis-stream localstack
docker rm lab2b-stream lab2b-kinesis-stream localstack
docker compose -f ../lab2a-kafka/docker-compose.yml -f ../lab2a-kafka/equipo/docker-compose.override.yml down
```

Los volúmenes `lab2b-lake` (tablas y checkpoints) y `lab2b-jars` (conector) quedan; se borran
con `docker volume rm lab2b-lake lab2b-jars` si ya no se necesitan.

## Desviaciones respecto al enunciado

### 1. La Parte 4 corre sobre LocalStack, no sobre AWS

El Learner Lab permite `list-streams` pero niega `kinesis:CreateStream` en `us-east-1` y
`us-west-2` (`no identity-based policy allows the kinesis:CreateStream action`). Es el caso del
Troubleshooting #6. LocalStack emula la API de Kinesis: el productor `boto3`, el conector de
Spark y el checkpoint hablan con ella igual que con AWS. Lo que se pierde es la consola (Data
Viewer y métricas de CloudWatch). No se creó ningún recurso en AWS.

### 2. Conector de AWS Labs en lugar del de Qubole

`crear_stream_kinesis()` usa `format("aws-kinesis")` con opciones `kinesis.*`, que son del
conector de AWS Labs. El paquete del README (`com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0`)
está hecho para Spark 3.0 y registra el formato `kinesis`, así que no corresponde con el código.
El de AWS Labs no está en Maven Central; se descarga como JAR y el script lo carga con
`spark.jars` cuando `STREAM_SOURCE=kinesis`.

### 3. Cambios en `streaming_pipeline.py` fuera de los TODO

Todos marcados con comentario en el código:

- Variables `KINESIS_ENDPOINT` y `KINESIS_CONNECTOR_JAR`.
- `spark.jars` con el conector cuando `STREAM_SOURCE=kinesis`.
- En `crear_stream_kinesis()`, `.option("kinesis.endpointUrl", ...)` solo si existe
  `KINESIS_ENDPOINT`. Sin esa variable, la función es la del profesor.

`aplicar_ventana()` y `escribir_batch()` son las mismas para Kafka y para Kinesis.

### 4. Alias de red para el endpoint de LocalStack

Con `kinesis.endpointUrl`, el conector saca la región del nombre del host aunque se le pase
`kinesis.region`, y falla con `http://localstack:4566` (`Cannot parse region`). Se le da a
LocalStack el alias `kinesis.us-east-1.localstack` en la red de Docker. Contra AWS real no hace
falta.

### 5. Tabla separada para Kinesis

La fuente Kinesis escribe en `/tmp/lake/silver/ventas_streaming_kinesis`, no en la ruta por
defecto, para no mezclar sus agregados con los de Kafka en la misma llave ventana+región.

## Notas para quien lo repita

- **Carpeta de trabajo.** El pipeline y `leer_ventanas.py` se lanzan desde `lab2b-kinesis`; el
  productor de Kafka desde `lab2a-kafka`. `${PWD}` en el `-v` depende de eso: desde la carpeta
  equivocada sale `can't open file '/scripts/...'`.
- **Ctrl+C en contenedor.** `docker kill --signal=SIGINT` no produce una salida limpia: la señal
  también le llega a la JVM y el script termina con `Py4JError`. El checkpoint no se daña.
- **Borrar el checkpoint** (`/tmp/lake/checkpoints/lab2b-kafka`) hace que Spark relea el topic
  desde `earliest`. Es seguro por el `MERGE`; necesario si se cambia la ventana o el watermark.
