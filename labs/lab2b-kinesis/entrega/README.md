# Lab 2b — Streaming con Spark Structured Streaming · Entrega

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

**Parte 4 (Kinesis, opcional):** no realizada, por decisión del equipo. No creamos
ningún recurso en AWS (ni stream de Kinesis ni checkpoint en S3), así que no hay
nada que limpiar. Lo que encontramos al revisarla está en la incidencia 4.

## Contenido de la entrega

```
entrega/
├── scripts/
│   └── streaming_pipeline.py   # TODO de las Partes 1, 2 y 3 resueltos
├── streaming_design.md         # Preguntas 1-6
├── bitacora_delegacion.md
└── README.md                   # este archivo
```

## Configuración del pipeline

| Componente | Valor |
|---|---|
| Fuente | Topic `pedidos-ventas` del Lab 2a: 4 particiones, replicación 1, `cp-kafka:7.6.0` en modo KRaft |
| Motor | Spark 3.5.3 (contenedor `apache/spark:3.5.3-python3`), `delta-spark` 3.1.0, conector `spark-sql-kafka-0-10_2.12:3.5.3` |
| Tiempo de evento | `kafka_time` = columna `timestamp` del mensaje en Kafka |
| Ventana / watermark | **5 minutos / 10 minutos** |
| Trigger / output mode | `processingTime="30 seconds"` / `update` |
| Sink | `foreachBatch` → `MERGE` Delta por `(window_start, window_end, region)` |
| Silver | `/tmp/lake/silver/ventas_streaming` |
| Checkpoint | `/tmp/lake/checkpoints/lab2b-kafka` |
| Particiones de estado | 200 (el valor por defecto de `spark.sql.shuffle.partitions`) |

## Cómo lo corrimos

**Infraestructura:** la del Lab 2a, desde `labs/lab2a-kafka/`, con `docker compose up -d`. Necesita el `docker-compose.override.yml` que documentamos en esa entrega, porque el `CLUSTER_ID` original no es válido para KRaft.

**Spark:** PySpark + Delta no corren en el host Windows (ver la entrega del Lab 2a), así que el pipeline corre en un contenedor Linux conectado a la red de Kafka:

```bash
docker run -d --name st1630-lab2b-spark --network lab2a-kafka_default -u root \
  -v "<ruta-del-repo>:/repo" -v lab2a-lake:/tmp/lake \
  -e KAFKA_BOOTSTRAP=kafka:29092 \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  apache/spark:3.5.3-python3 sleep infinity
docker exec st1630-lab2b-spark python3 -m pip install --no-deps delta-spark==3.1.0 importlib_metadata

docker exec -w /repo/labs/lab2b-kinesis/entrega/scripts st1630-lab2b-spark python3 streaming_pipeline.py
```

Se lanza con `python3` y no con `spark-submit` a propósito: el script agrega los JAR de Delta y del conector de Kafka con `configure_spark_with_delta_pip(..., extra_packages=[...])`, y esa configuración solo surte efecto cuando PySpark arranca la JVM él mismo. No hubo que modificar el script: `KAFKA_BOOTSTRAP` ya venía parametrizado por variable de entorno.

**Productor:** el `productor_kafka.py` de nuestra entrega del Lab 2a (1.000 pedidos con `key=region`), corrido desde el host.

**Verificación de Silver:** después de cada corrida del productor leímos la tabla y contamos filas contra llaves distintas:

```python
df = spark.read.format("delta").load("/tmp/lake/silver/ventas_streaming")
filas = df.count()
llaves = df.select("window_start", "window_end", "region").distinct().count()
# si el MERGE es idempotente: filas == llaves
```

## Incidencias

1. **El topic arrancó vacío.** La retención del topic es de 7 días (`retention.ms=604800000`), y el broker borró los mensajes del Lab 2a (del 31/08) apenas se levantó: `Incremented log start offset to 316 due to segment deletion`. `kafka-get-offsets` seguía mostrando 543/316/58/83 porque reporta el offset **final**; con `--time -2` (el offset más antiguo) daba lo mismo, o sea, cero mensajes disponibles. Para este lab fue una ventaja: no quedaron ventanas viejas en Silver.
2. **El productor que indica el README no corre en `master`.** `../lab2a-kafka/scripts/productor_kafka.py` todavía tiene el TODO sin resolver; usamos la versión completa de nuestra entrega del Lab 2a.
3. **Git Bash reescribe rutas en `docker exec -e`.** Pasar `-e CHECKPOINT_PATH=/tmp/lake/...` desde Git Bash la convierte en `C:/Users/.../Temp/lake/...`, y Spark falla con `No FileSystem for scheme "C"`. Se evita anteponiendo `MSYS_NO_PATHCONV=1` (o lanzando desde PowerShell).
4. **La Parte 4 no funciona tal como viene.** Lo encontramos al revisarla, antes de decidir no hacerla. `crear_stream_kinesis()` usa `format("aws-kinesis")` con opciones `kinesis.region`, `kinesis.streamName` y `kinesis.consumerType`, que son de la API del conector de AWS Labs. El paquete que indica el README, `com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0`, registra el formato `"kinesis"` y usa opciones `streamName`, `endpointUrl` y `startingposition`, así que con ese JAR el `.load()` no encuentra la fuente. Además, `kinesis_producer.py` publica un JSON con menos campos que `PEDIDO_SCHEMA` (no trae `fecha`, `producto`, `cantidad`, `precio_unit`, `canal` ni `metodo_pago`).

## Resultados

- **4 corridas del productor**, de 1.000 pedidos cada una. Silver terminó con **18 filas y 18 llaves distintas: 0 duplicados.**
- **Ventanas que crecen en su lugar:** en la ventana 18:45–18:50, dos corridas seguidas llevaron las mismas 6 filas de 1.000 a 2.000 pedidos. En la ventana 18:25–18:30, una sola corrida quedó partida entre dos micro-batches (866 → 1.000 pedidos).
- **Historial Delta:** 1 `WRITE` y 5 `MERGE`. Los MERGE que traían llaves nuevas insertaron 6 filas y actualizaron 0; los que traían llaves existentes actualizaron 6 e insertaron 0.
