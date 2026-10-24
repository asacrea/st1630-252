# Lab 2a — Productor/consumidor Kafka · Entrega

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 31/08/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

## Contenido de la entrega

```
entrega/
├── scripts/
│   ├── productor_kafka.py      # TODO 1.1, 1.3, 1.4 resueltos
│   └── consumidor_kafka.py     # TODO 2.1, 2.2/2.3 resueltos
├── datos/
│   ├── prueba_idempotencia.md  # la prueba con evidencia real
│   └── kafka_ui_lag_cero.png   # captura del Consumer Group con lag = 0
├── kafka_design.md             # las 5 preguntas + anexo Parte 0
├── bitacora_delegacion.md
└── README.md                   # este archivo
```

## Configuración de nuestro clúster

| Componente | Valor |
|---|---|
| Imagen del broker | `confluentinc/cp-kafka:7.6.0` |
| Modo | KRaft (`broker,controller` en el mismo proceso, `NODE_ID=1`) |
| Quórum | `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:29093`, `CurrentVoters: [1]` |
| ClusterId | `li8qjcjaTny7YQ_IRHUdmg` |
| Listeners | `EXTERNAL://localhost:9092` (host), `PLAINTEXT://kafka:29092` (red Docker), `CONTROLLER://:29093` |
| Topic | `pedidos-ventas` — 4 particiones, replicación 1, todas con `Leader: 1`, `Isr: 1` |
| Consumer group | `analytics-group`, `enable_auto_commit=False` |
| Bronze | Delta Lake en `/tmp/lake/bronze/pedidos` (volumen Docker `lab2a-lake`) |
| Kafka UI | `provectuslabs/kafka-ui` en http://localhost:8080 |

## Cómo corrimos cada parte

**Infraestructura y topic (Parte 0)** — desde `labs/lab2a-kafka/`:

```bash
docker compose up -d
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas \
  --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
```

**Productor (Parte 1)** — en el host (Windows), con un venv de Python 3.12
y `kafka-python==2.2.15`:

```bash
python entrega/scripts/productor_kafka.py
```

Resultado: 1.000 mensajes en **20,4 s**, resumen región→partición al
final del log (`datos/log_productor.txt`).

**Consumidor (Parte 2)** — dentro de un contenedor Linux con Spark 3.5.3
(ver la sección de incidencias, más abajo, para el porqué):

```bash
docker exec st1630-lab2a-spark bash -lc "cd /work/entrega/scripts && \
  /opt/spark/bin/spark-submit --master 'local[2]' \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.shuffle.partitions=1 \
    --conf spark.databricks.delta.optimizeWrite.enabled=true \
    --conf spark.databricks.delta.autoCompact.enabled=true \
    consumidor_kafka.py"
```

El contenedor recibe `KAFKA_BOOTSTRAP=kafka:29092` y
`BRONZE_PATH=/tmp/lake/bronze/pedidos` como variables de entorno — los
dos scripts ya estaban parametrizados con `os.environ.get(...)`, así que
**no hubo que tocar una sola línea de código** para moverlos de un
entorno a otro. Es el mismo mecanismo con el que apuntarían a S3 en
producción.

## Incidencias del entorno y cómo las resolvimos

Ninguna de estas tres es un problema del lab en sí, pero las
documentamos porque nos costaron tiempo y porque la primera afecta a
cualquiera que use el `docker-compose.yml` tal cual viene.

### 1. El broker no arrancaba: `CLUSTER_ID` inválido

El `docker-compose.yml` del curso trae
`CLUSTER_ID: "st1630lab2aKRaftClusterID"` (25 caracteres). KRaft exige
un UUID de 16 bytes codificado en base64 url-safe sin padding, o sea
exactamente 22 caracteres, y el preflight aborta:

```
===> Using provided cluster id st1630lab2aKRaftClusterID ...
Cluster ID string st1630lab2aKRaftClusterID does not appear to be a valid UUID:
Input string with prefix `st1630lab2aKRaftClusterI` is too long to be decoded as a base64 UUID
```

Como el enunciado pide no modificar el compose, lo arreglamos con un
**`docker-compose.override.yml`** (en `labs/lab2a-kafka/`) que solo pisa
esa variable. El archivo original queda intacto:

```yaml
services:
  kafka:
    environment:
      CLUSTER_ID: "li8qjcjaTny7YQ_IRHUdmg"
```

Después de crear el override hay que borrar el volumen a medio formatear:
`docker compose down -v && docker compose up -d`.

### 2. PySpark no corre con Python 3.13 (ni 3.12) en el host

Con Python 3.13 y 3.12, los *python workers* de PySpark 3.5.3 se caían
(`Python worker exited unexpectedly (crashed)` / `EOFException`) apenas
la ejecución tocaba código Python distribuido. PySpark 3.5 soporta
oficialmente hasta Python 3.11. Un venv con **Python 3.9** resolvió esa
parte.

### 3. Delta sobre Windows: `NativeIO$Windows.access0`

Ya con Python 3.9, escribir Delta desde Windows falla porque Hadoop
necesita `winutils.exe` + `hadoop.dll`. Instalarlos y definir
`HADOOP_HOME` quita el primer error, pero aparece otro que no depende de
nosotros:

```
java.lang.UnsatisfiedLinkError:
'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
```

Probamos las builds de `hadoop.dll` 3.1.2, 3.2.2, 3.3.5 y 3.3.6 y todas
dan el mismo desajuste contra el Hadoop que empaqueta Spark 3.5.3.

**Solución:** correr el consumidor en Linux, dentro de un contenedor
`apache/spark:3.5.3-python3` conectado a la misma red Docker que el
broker. Es más parecido a producción que el host, y no exigió cambiar el
script:

```bash
docker run -d --name st1630-lab2a-spark \
  --network lab2a-kafka_default -u root \
  -v "<ruta>/labs/lab2a-kafka":/work -v lab2a-lake:/tmp/lake \
  -e KAFKA_BOOTSTRAP=kafka:29092 -e BRONZE_PATH=/tmp/lake/bronze/pedidos \
  apache/spark:3.5.3-python3 sleep infinity
docker exec st1630-lab2a-spark python3 -m pip install --no-deps \
  kafka-python==2.2.15 delta-spark==3.1.0 importlib_metadata
```

El productor sí corrió en el host (no usa Spark).

### 4. Un MERGE por mensaje se degrada rápido

Con la implementación del lab (un `MERGE` Delta por mensaje), el
consumidor arrancó en ~3 s/mensaje y a los ~90 mensajes ya iba en
~5,9 s/mensaje: cada mensaje agrega un commit al `_delta_log` y un
archivo Parquet minúsculo, y el MERGE siguiente tiene que listarlos y
escanearlos todos. Con más de 1.000 archivos, un solo MERGE llegó a
tardar **minutos**.

Lo resolvimos con `scripts/mantenimiento_bronze.py`, que marca la tabla
con `delta.autoOptimize.*` y corre `OPTIMIZE` + `VACUUM` (1.049 archivos
→ 1). No es parte de la rúbrica: es mantenimiento de la tabla, sin tocar
el código del consumidor. La solución de fondo — micro-batches en vez de
un MERGE por mensaje — está discutida en la Pregunta 5 de
`kafka_design.md`.

### 5. `CommitFailedError` en cascada: el consumidor expulsado del grupo

Esta fue la falla más interesante del lab, y es consecuencia directa de
la anterior. Dos veces (`datos/log_consumidor_run4_drenaje.txt`, 292
errores; `datos/log_consumidor_run5_drenaje.txt`, 213 errores) el
consumidor se puso a escupir:

```
[ERROR] offset=535 partition=0 no se commiteó -- se reprocesará. Causa: CommitFailedError:
[ERROR] offset=536 partition=0 no se commiteó -- se reprocesará. Causa: CommitFailedError:
```

**Causa:** `KafkaConsumer` trae `max_poll_records=500` por defecto. El
iterador trae un lote de hasta 500 mensajes en un solo `poll()` y
nuestro loop los procesa de a uno, con un MERGE de ~2 s cada uno: unos
15 minutos sin volver a llamar a `poll()`. Como eso supera
`max_poll_interval_ms` (5 min por defecto), el broker da por muerto al
consumidor, lo saca del grupo y le reasigna las particiones — y a partir
de ahí **todo `commit()` falla**, porque el consumidor ya no es dueño de
la partición que quiere commitear.

Lo interesante es que el MERGE seguía funcionando: Bronze llegó a tener
539 filas mientras el offset commiteado del grupo seguía clavado en 246.
Es decir, ~293 mensajes escritos en Bronze sin commit. Y eso es
justamente lo que el diseño at-least-once promete: al reiniciar, Kafka
reentregó esos 293 mensajes, el MERGE los absorbió y Bronze **no creció
ni una fila** por ellos (ver `datos/prueba_idempotencia.md`).

**Arreglo:** `max_poll_records=10` en el `KafkaConsumer` — el único
parámetro que agregamos por fuera de lo que pedía el TODO 2.1, y está
comentado en el propio script. Con lotes de 10, `poll()` se vuelve a
llamar cada ~20 s. Después del cambio: 0 `CommitFailedError`.

## Resultados

- **Productor:** 1.000 pedidos publicados con `key=region` y
  `acks='all'`; cada región siempre a la misma partición; P0 concentró
  el 54,3 % del tráfico (Bogotá + Cali).
- **Consumidor:** `enable_auto_commit=False`, commit manual después del
  MERGE, y las 4 columnas de trazabilidad `_kafka_offset`,
  `_kafka_partition`, `_kafka_topic`, `_ingested_at` en cada fila de
  Bronze.
- **Idempotencia (prueba dirigida):** 6 mensajes reentregados y
  reprocesados; Bronze pasó de 17 filas a 17 filas, 0 duplicados.
- **Idempotencia (a escala completa):** entre las 6 corridas del
  consumidor se ejecutaron **1.512 MERGE para 1.000 mensajes únicos**
  (512 reprocesamientos, la mayoría por los `CommitFailedError` de la
  incidencia 5). Estado final: **1.000 filas, 1.000 `pedido_id`
  distintos, 0 duplicados**, y `lag = 0` en las 4 particiones. Detalle
  en `datos/prueba_idempotencia.md`.
