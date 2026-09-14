# Lab 2a · Productor/consumidor Kafka — entrega del equipo

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 2026-08-31
**Estudiantes:**
Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co · Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
Sebastián Durán Fernández — sduranf@eafit.edu.co · Daniel Arcila Salazar — darcilas1@eafit.edu.co

## Qué hay en esta carpeta

```
labs/lab2a-kafka/equipo/
├── README.md                          # este archivo
├── kafka_design.md                    # las 5 preguntas + anexo de la Parte 0
├── bitacora_delegacion.md
├── docker-compose.override.yml        # corrige el CLUSTER_ID (ver abajo)
├── Dockerfile.spark                   # entorno del consumidor (ver abajo)
├── scripts/
│   ├── productor_kafka.py             # TODO 1.1, 1.3 y 1.4 implementados
│   ├── consumidor_kafka.py            # TODO 2.1, 2.2 y 2.3 implementados
│   ├── contar_bronze.py               # utilidad propia de verificación
│   └── benchmark_sync_vs_async.py     # sección 1.5 (opcional)
└── datos/
    ├── prueba_idempotencia.md         # Parte 2.4, con evidencia real
    ├── kafka_ui_lag_cero.png          # Parte 2.5, Consumer Group con lag 0 por partición
    ├── kafka_ui_particiones.png       # Parte 0 paso 3, vista de particiones del topic
    ├── verificacion_final_bronze.txt  # conteo final: 1.000 filas, 0 duplicados
    ├── parte0_exploracion.txt         # evidencia cruda de la Parte 0
    ├── parte0_consumer_offsets.txt    # aparición de __consumer_offsets
    ├── productor_output.txt           # salida completa del productor
    ├── consumidor_output.txt          # salida completa del consumidor (947 OK)
    ├── consumidor_regimen1_sin_tuning.txt  # la degradación 5 s -> 20 s
    ├── consumidor_expulsion_evidencia.txt  # el CommitFailedError
    └── benchmark_sync_vs_async.txt    # resultado del benchmark 1.5
```

## Configuración de nuestro clúster

| | |
|---|---|
| Kafka | `confluentinc/cp-kafka:7.6.0`, modo **KRaft** (broker + controller en un nodo) |
| Broker | `KAFKA_NODE_ID=1`, listener externo `localhost:9092`, interno `kafka:29092`, quórum `kafka:29093` |
| Topic | `pedidos-ventas` — **4 particiones**, factor de replicación **1** |
| Consumer group | `analytics-group` — 1 consumidor |
| Bronze | tabla Delta en `/lake/bronze/pedidos`, sobre un volumen Docker nativo (`lab2a-lake-vol`) |
| Spark / Delta | PySpark **3.5.6** + delta-spark **3.3.0** (Scala 2.12), Java 17 |
| kafka-python | **3.0.11** |
| Host | Windows 11, Docker Desktop 28.4.0, Python 3.13 en venv |

## Resultados

| Métrica | Valor |
|---|---|
| Mensajes publicados por el productor | **1.000** |
| Distribución por partición | P0=556, P1=263, P2=85, P3=96 |
| Partición más cargada (hot partition) | **P0 con 55,6 %** (Bogotá 403 + Cali 153) |
| Filas finales en Bronze | 1.000, con 1.000 `pedido_id` distintos — **0 duplicados** |
| Prueba de idempotencia | 6 mensajes reprocesados → **+5 filas** (ver `datos/prueba_idempotencia.md`) |
| Lag final del consumer group | **0** en las 4 particiones |
| Mensajes procesados en la corrida final | 947, **0 errores** |
| Benchmark 1.5 (opcional) | síncrono 16,35 s vs asíncrono 0,56 s → **29,25× más rápido** |

### El resultado que más dice del pipeline

Bronze quedó con **1.000 filas y 1.000 `pedido_id` distintos — 0 duplicados**,
a pesar de que el pipeline se interrumpió **cuatro veces** durante el lab, y
cada interrupción provocó reentregas reales de Kafka:

1. `SIGKILL` deliberado a los 15 mensajes (la prueba de idempotencia).
2. Corte manual tras 6 mensajes para medir N'.
3. Expulsión del consumer group por `max_poll_interval_ms`: 10 mensajes con
   el MERGE hecho y el commit rechazado.
4. Reinicio para aplicar el tuning y mover el datalake a un volumen Docker.

Sumando las corridas se ejecutaron más de 1.000 procesamientos sobre un
topic de 1.000 mensajes, con decenas de reprocesos **reales**, no simulados.
Y Bronze no duplicó una sola fila. Eso es at-least-once + MERGE idempotente
funcionando de punta a punta. Detalle en `datos/verificacion_final_bronze.txt`.

## Cuatro desviaciones respecto al enunciado, y por qué

### 1. El `CLUSTER_ID` del `docker-compose.yml` no arranca

El archivo original trae `CLUSTER_ID: "st1630lab2aKRaftClusterID"`. En modo
KRaft el broker no acepta un string arbitrario: el id tiene que ser un UUID
de 128 bits en base64 sin padding, o sea **exactamente 22 caracteres**. El
del compose tiene 25, así que el preflight aborta el arranque:

```
===> Using provided cluster id st1630lab2aKRaftClusterID ...
Cluster ID string st1630lab2aKRaftClusterID does not appear to be a valid UUID:
Input string with prefix `st1630lab2aKRaftClusterI` is too long to be decoded
as a base64 UUID
```

Como el README del lab pide **no modificar** el `docker-compose.yml`, en vez
de editarlo dejamos un `docker-compose.override.yml` en esta carpeta que
sobreescribe solo esa variable. El UUID lo generó el propio Kafka:

```bash
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
# -> OOiHFDxNTKunDxTBeHYiRg   (22 caracteres)
```

Este tropiezo terminó siendo la mejor evidencia de la Pregunta 4(c): ese
error **solo existe en modo KRaft**, porque es el formateo del log de
metadatos del quórum Raft dentro del propio broker.

### 2. El consumidor corre en un contenedor, no en el host

El productor corre nativo en Windows sin problema (solo usa
`kafka-python`). El consumidor no, porque necesita PySpark, y **PySpark no
puede escribir en disco en Windows sin `winutils.exe` / `hadoop.dll`**.
Probamos esa ruta: con `HADOOP_HOME` apuntando a los binarios de
`hadoop-3.3.5` y los JARs de Hadoop 3.3.4 que trae PySpark 3.5.6, la carga
nativa falla con

```
java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0
```

En lugar de seguir dependiendo de binarios de terceros no oficiales, el
consumidor corre en Linux dentro del contenedor definido en
`Dockerfile.spark`. Las variables de entorno que el propio script ya expone
(`KAFKA_BOOTSTRAP`, `BRONZE_PATH`) son justamente lo que hace que esto no
requiera tocar el código: dentro de la red de Docker el bootstrap es
`kafka:29092` en vez de `localhost:9092`.

### 3. `max_poll_records=1` en el consumidor

Este es un ajuste que **no venía en el enunciado** y que hubo que agregar
para que el lab terminara. Con el valor por defecto (500), el consumidor se
trae 500 mensajes en un `poll()` y no vuelve a llamar a `poll()` hasta
procesarlos todos. Como cada mensaje cuesta un MERGE Delta de ~5 s, entre
dos `poll()` pasaban ~40 minutos, muy por encima de `max_poll_interval_ms`
(300 s por defecto). El broker daba al consumidor por muerto, lo expulsaba
del grupo, y **todos los commits siguientes fallaban**:

```
[ERROR] offset=44 partition=1 no se commiteó -- se reprocesará. Causa:
CommitFailedError: Offset commit cannot be completed since the consumer is
not part of an active group for auto partition assignment; it is likely that
the consumer was kicked out of the group.
```

La evidencia completa está en `datos/consumidor_expulsion_evidencia.txt`
(10 errores consecutivos). Con `max_poll_records=1`, entre `poll()` pasan
~5 s y la sesión nunca expira.

Vale la pena notar que este es un **parche, no la solución de fondo**: bajar
`max_poll_records` arregla el timeout pero deja el throughput en el suelo.
La solución real para volumen alto es procesar por lotes (un MERGE cada
5.000 mensajes en vez de uno por mensaje), que es justamente lo que
argumentamos en la Pregunta 5(c) de `kafka_design.md`.

### 4. Tuning de Spark/Delta, porque el lab no terminaba

Con la configuración del enunciado tal cual, el consumidor **no alcanza a
procesar los 1.000 mensajes**. El tiempo por mensaje no es constante: crece.

| Momento | Mensajes ya en Bronze | Tiempo por mensaje |
|---|---|---|
| Arranque | ~15 | **~5 s** |
| A los ~120 mensajes | 124 | **~20 s** |

La causa es el **problema de *small files***: cada mensaje se escribe en su
propio archivo Parquet de una fila, y el MERGE tiene que escanear todos los
archivos existentes para buscar el match por `pedido_id`. Medido:
**124 mensajes → 124 archivos Parquet, 3,0 MB**. El costo por mensaje crece
con lo ya ingestado, o sea que el total es cuadrático. Extrapolando el
ritmo de 20 s, los 1.000 mensajes habrían tardado más de 5 horas, y
subiendo.

Se agregaron tres `.config()` al `SparkSession` (la lógica del MERGE no se
tocó):

```python
.config("spark.sql.shuffle.partitions", "4")            # default 200
.config("spark.databricks.delta.optimizeWrite.enabled", "true")
.config("spark.databricks.delta.autoCompact.enabled", "true")
```

`shuffle.partitions=4` porque el default de 200 lanza 200 tareas de shuffle
para hacer el join de **una** fila contra la tabla: el scheduling dominaba
el tiempo del MERGE. `optimizeWrite` y `autoCompact` atacan la causa de
fondo, compactando los archivos pequeños para que el scan deje de crecer.

Además el datalake se movió de un **bind mount de Windows** a un **volumen
Docker nativo**: el I/O a través del sistema de archivos virtualizado de
Docker Desktop en Windows era una parte importante del costo.

**Resultado: de ~20 s a ~3 s por mensaje** (6-7× más rápido) y, sobre todo,
sin la degradación progresiva.

## Cómo correr esta entrega

Todo se ejecuta desde `labs/lab2a-kafka/`.

### 1. Levantar el clúster

```bash
docker compose -f docker-compose.yml -f equipo/docker-compose.override.yml up -d
docker ps    # esperar a que st1630-lab2a-kafka diga "Up (healthy)"
```

### 2. Crear el topic (Parte 0)

```bash
docker exec st1630-lab2a-kafka kafka-topics --create \
  --topic pedidos-ventas --partitions 4 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 3. Productor (nativo en el host)

```bash
python -m venv .venv-lab2a
./.venv-lab2a/Scripts/python.exe -m pip install "kafka-python>=2.1"
PYTHONIOENCODING=utf-8 ./.venv-lab2a/Scripts/python.exe equipo/scripts/productor_kafka.py
```

### 4. Consumidor (en contenedor)

```bash
docker build -f equipo/Dockerfile.spark -t st1630-lab2a-spark equipo/

docker run -d --name lab2a-cons --network lab2a-kafka_default \
  -v "$PWD/equipo/scripts:/scripts" \
  -v "C:/Users/sebas/lab2a-lake:/lake" \
  -v lab2a-ivy:/root/.ivy2 \
  -e KAFKA_BOOTSTRAP=kafka:29092 \
  -e BRONZE_PATH=/lake/bronze/pedidos \
  -e PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.3.0 pyspark-shell" \
  st1630-lab2a-spark python /scripts/consumidor_kafka.py

docker logs -f lab2a-cons
```

> El `-v lab2a-ivy:/root/.ivy2` es solo caché: evita que Spark vuelva a
> bajar los JARs de Delta desde Maven en cada ejecución.

### 5. Verificar Bronze

```bash
docker run --rm --network lab2a-kafka_default \
  -v "$PWD/equipo/scripts:/scripts" \
  -v "C:/Users/sebas/lab2a-lake:/lake" -v lab2a-ivy:/root/.ivy2 \
  -e BRONZE_PATH=/lake/bronze/pedidos \
  -e PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.3.0 pyspark-shell" \
  st1630-lab2a-spark python /scripts/contar_bronze.py "verificacion final"
```

### 6. Estado del consumer group

```bash
docker exec st1630-lab2a-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group analytics-group
```

Kafka UI queda en <http://localhost:8080>.

## Bitácora de delegación

Ver `bitacora_delegacion.md`.
