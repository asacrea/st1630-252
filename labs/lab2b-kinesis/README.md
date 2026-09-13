# Lab 2b · Streaming con Spark Structured Streaming (Kafka + Kinesis opcional)

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Última edición:** 2026-08-24
**Peso:** parte del 30% de laboratorios (ver `../../docs/evaluacion.md`)
**Tiempo estimado:** 6-8 horas, distribuidas en 3 días
**Entrega:** Pull Request antes del inicio de S8

> **Este es un laboratorio 100% en casa.** Lo haces de forma autónoma,
> sin sesión de clase — esta guía debe bastarte por sí sola. Si te
> atoras, revisa primero la sección de Troubleshooting antes de
> escribir al profesor.
>
> **Nota de alcance:** este lab cubre Spark Structured Streaming sobre
> Kafka (obligatorio) y una variante opcional evaluable sobre Kinesis
> Data Streams. La integración con Elastic Stack mencionada en el
> nombre original del lab no está incluida en esta versión — pregunta
> al profesor si tu cohorte la necesita como parte adicional.

## Objetivo

Reemplazar el consumidor manual del Lab 2a (`KafkaConsumer` +
`commit()` explícito, mensaje por mensaje) por **Spark Structured
Streaming**: una capa que administra la lectura, el progreso y la
tolerancia a fallos por ti, mientras tú te concentras en describir
**qué** calcular — ventanas de tiempo con watermark — y **cómo**
escribir el resultado de forma idempotente (mismo patrón `MERGE`
Delta de los labs anteriores).

## Prerequisito y verificación

**Infraestructura del Lab 2a** (este lab la reutiliza, no crea una
nueva):

```bash
cd ../lab2a-kafka
docker-compose up -d
docker ps | grep kafka   # st1630-lab2a-kafka y st1630-lab2a-kafka-ui, ambos Up
```

Si el topic `pedidos-ventas` no existe todavía en tu entorno, créalo
(Lab 2a, Parte 0):

```bash
docker exec st1630-lab2a-kafka kafka-topics --create \
  --topic pedidos-ventas --partitions 4 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

**Software adicional para este lab:**

```bash
python3 -c "import pyspark, delta; print('pyspark/delta OK')"
```

**Conceptual:** debes tener fresco el Lab 2a completo — en particular
por qué `enable_auto_commit=False` + commit manual te da at-least-once
real, y por qué el `MERGE` por `pedido_id` es idempotente. Este lab
construye directamente sobre esas dos ideas, solo que ahora las
implementa Spark en vez de tu propio loop.

## Arquitectura del lab

```
  [productor_kafka.py del Lab 2a]  ──▶  topic "pedidos-ventas"
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     │                                                  │
                     ▼ (obligatorio)                     (opcional, Parte 4) ▼
        ┌───────────────────────┐                    ┌───────────────────────┐
        │ crear_stream_kafka()  │                    │ crear_stream_kinesis()│
        │ readStream de Kafka   │                    │ readStream de Kinesis │
        └───────────┬───────────┘                    └───────────┬───────────┘
                     │                                            │
                     └─────────────────┬──────────────────────────┘
                                        ▼
                          aplicar_ventana(df)
                     withWatermark + groupBy(window, region) + agg
                                        │
                                        ▼
                          escribir_batch(micro_batch)
                    foreachBatch → MERGE por (window, region)
                    idempotente, checkpoint gestiona el progreso
                                        │
                                        ▼
                  [Delta — /tmp/lake/silver/ventas_streaming]
```

**El punto pedagógico central:** todo lo que pasa después de
`crear_stream_kafka()`/`crear_stream_kinesis()` — ventana, watermark,
sink — es exactamente el mismo código. Cambiar de transporte no
cambia la lógica de procesamiento. Eso es lo que vas a comprobar tú
mismo si haces la Parte 4.

## Parte 1 — Leer de Kafka con Structured Streaming (Día 1, 1.5 horas)

Abre `scripts/streaming_pipeline.py`, lee el docstring completo, y
completa el TODO de `crear_stream_kafka()`. El comentario justo antes
del TODO te explica cada pieza: por qué `spark.readStream` en vez de
`spark.read`, cómo decodificar la columna `value` (bytes → JSON), y
por qué la columna `timestamp` que trae Kafka de fábrica es tu
"tiempo de evento" para las ventanas de la Parte 2.

**Diferencia clave con el Lab 2a:** ahí controlabas tú mismo el loop
`for mensaje in consumer` y decidías cuándo leer el siguiente mensaje.
Acá describes de dónde leer y Spark administra el resto — vas a ver
esto reflejado en la Parte 2 (checkpoint reemplaza tu commit manual).

**Verifica** que compile corriendo el pipeline completo hasta el final
de la Parte 3 (no puedes probar `crear_stream_kafka()` de forma
aislada — Structured Streaming solo se "activa" con un sink real).

## Parte 2 — Ventana de tiempo y watermark (Día 2, 2 horas)

Completa `aplicar_ventana()`. El comentario del TODO explica el
trade-off de elegir un watermark corto vs. largo — tu trabajo es
elegir un tamaño de ventana y un watermark concretos y ser capaz de
justificarlos (Pregunta 1 de `streaming_design.md`).

Recuerda aplanar la columna `window` (un struct con `start`/`end`) a
dos columnas separadas `window_start` y `window_end` — las necesitas
tal cual en la Parte 3 para la condición del `MERGE`.

## Parte 3 — Sink foreachBatch + MERGE (Día 2, 2 horas)

Completa `escribir_batch()`. Este es el reemplazo directo de la
Parte 2.3 del Lab 2a (el MERGE por `pedido_id`), pero con una llave
distinta — el comentario del TODO explica por qué. Piensa en términos
de qué pasa si Spark te entrega la MISMA ventana+región en dos
micro-batches seguidos (porque siguen llegando pedidos para esa
ventana antes de que el watermark la cierre): tu MERGE debe
actualizarla, no duplicarla.

**Corre el pipeline completo** cuando termines los 3 TODO:

```bash
# Terminal 1 — asegúrate de tener datos en el topic
cd ../lab2a-kafka/scripts && python3 productor_kafka.py

# Terminal 2 — el pipeline de streaming
cd ../../lab2b-kinesis/scripts && python3 streaming_pipeline.py
```

**Para ver actualizaciones "en vivo" a una ventana ya abierta** (y no
solo un batch estático que se procesa una vez): con
`streaming_pipeline.py` corriendo, vuelve a correr
`productor_kafka.py` en la Terminal 1 — los pedidos nuevos van a tener
un `kafka_time` cercano a "ahora" y van a caer en la ventana actual,
actualizándola. Verifica contando filas en Delta antes y después:

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
spark.read.format("delta").load("/tmp/lake/silver/ventas_streaming").show(20, truncate=False)
```

**Verifica:** las columnas `ventas_totales`/`num_pedidos` de una
ventana ya existente deben *crecer* entre una corrida del productor y
la siguiente — si en cambio ves filas duplicadas para la misma
ventana+región, tu condición de `MERGE` no está bien construida
(revisa Troubleshooting).

## Parte 4 — Kinesis en AWS Academy (opcional evaluable, Día 3, 1.5 horas)

Esta parte reemplaza Kafka por Kinesis Data Streams como fuente del
pipeline. La lógica de Spark Structured Streaming es **idéntica** —
solo cambia el source (`crear_stream_kinesis()`, ya resuelta en el
script). Ese es el punto pedagógico: los conceptos de ventanas,
watermark y checkpointing son agnósticos del transporte.

### 4.1 Prerequisitos

Verifica acceso a AWS Academy antes de empezar:

```bash
aws kinesis list-streams --region us-east-1
# Output esperado: {"StreamNames": []}
```

Si da error de credenciales: renueva el token de AWS Academy (expira
cada 4 horas — renuévalo en el portal antes de arrancar).

### 4.2 Crear el stream en Kinesis Data Streams

```bash
aws kinesis create-stream \
  --stream-name pedidos-ventas-kinesis \
  --shard-count 2 \
  --region us-east-1

# Verificar que el stream está ACTIVE (tarda ~30 segundos):
aws kinesis describe-stream-summary \
  --stream-name pedidos-ventas-kinesis \
  --region us-east-1 \
  --query "StreamDescriptionSummary.StreamStatus"
```

¿Por qué 2 shards? El productor de este lab envía ~10 pedidos/segundo
a razón de ~200 bytes cada uno (~2 KB/s) — muy por debajo del límite
de 1 MB/s de escritura por shard. Con 1 shard sería más que suficiente
para el volumen del lab; usamos 2 para que puedas observar paralelismo
real entre shards en el Data Viewer de la consola. Costo estimado en
AWS Academy: mínimo (créditos cubiertos, pero no olvides el paso 4.6).

### 4.3 Producer — `kinesis_producer.py`

Ya está resuelto en `scripts/kinesis_producer.py` — envía 500 pedidos
sintéticos al stream con `PartitionKey=region` (la misma decisión de
diseño que `key=region` en el Lab 2a). Ejecuta:

```bash
pip install boto3   # si no lo tienes
python3 scripts/kinesis_producer.py
```

Compara mentalmente contra tu `productor_kafka.py` del Lab 2a — es la
base de la Pregunta 7(a)/(b) de `streaming_design.md`.

### 4.4 Leer de Kinesis con Spark Structured Streaming

`crear_stream_kinesis()` en `streaming_pipeline.py` ya está resuelta.
Requiere el conector de Kinesis para Spark:

```bash
STREAM_SOURCE=kinesis spark-submit \
  --packages com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0 \
  scripts/streaming_pipeline.py
```

`--packages` descarga el conector desde Maven Central la primera vez
que lo usas — necesitas conexión a internet para esa descarga inicial
(se cachea localmente después). El resto del pipeline
(`aplicar_ventana`, `escribir_batch`) es exactamente el mismo código
que ya escribiste para Kafka — no lo dupliques ni lo reescribas.

### 4.5 Verificar en AWS Console

- **Kinesis → Data Streams → pedidos-ventas-kinesis**
- Tab **"Monitoring"**: revisa `PutRecords`, `GetRecords.IteratorAgeMilliseconds`
  (el `IteratorAge` es el equivalente de "lag" de un consumer group de Kafka
  — qué tan atrasado está tu consumidor respecto al dato más reciente).
- Tab **"Data viewer"**: selecciona un shard y observa los mensajes en
  tiempo real (el equivalente de Kafka UI).

**Captura obligatoria** (si haces esta parte): el Data Viewer
mostrando mensajes JSON reales de tu producer — guárdala en tu carpeta
de entrega.

### 4.6 Limpiar recursos al terminar

⚠️ **Obligatorio al finalizar** — aunque sean créditos de AWS Academy:

```bash
aws kinesis delete-stream --stream-name pedidos-ventas-kinesis --region us-east-1

# Verificar eliminación (puede tardar 1-2 minutos):
aws kinesis list-streams --region us-east-1

# Eliminar el checkpoint de Kinesis si lo dejaste en S3:
aws s3 rm s3://<tu-bucket>/checkpoints/lab2b-kinesis/ --recursive
```

### 4.7 Pregunta de análisis Kinesis

Responde la Pregunta 7 de `streaming_design.md` (comparativa Kafka vs.
Kinesis) — solo si hiciste esta Parte 4.

## Entregable — estructura del PR

```
labs/lab2b-kinesis/
└── <tu-nombre>/
    ├── scripts/
    │   ├── streaming_pipeline.py       # con tus 3 (o 4, si hiciste Kinesis) TODO completos
    │   └── kinesis_producer.py         # solo si hiciste la Parte 4
    ├── datos/
    │   └── kinesis_data_viewer.png     # solo si hiciste la Parte 4
    ├── streaming_design.md             # Preguntas 1-6 (+ 7 si aplica)
    ├── bitacora_delegacion.md
    └── README.md                       # tu nombre, fecha, si hiciste o no la Parte 4
```

Abre el PR hacia `main` con título
`lab(2b): <tu nombre> — streaming pipeline`, antes del inicio de S8.

## Rúbrica de evaluación

| Criterio | Peso | Completo | Parcial | Incompleto |
|---|---|---|---|---|
| **Lectura Kafka + ventana** (Partes 1-2) | 30% | `crear_stream_kafka()` y `aplicar_ventana()` completos, con ventana y watermark justificados | Funciona pero sin watermark, o con una ventana no justificada | No existe o no compila |
| **Sink idempotente** (Parte 3) | 30% | `escribir_batch()` hace `MERGE` por `(window_start, window_end, region)`, verificado con al menos 2 corridas del productor sin duplicar filas | `MERGE` presente pero con llave incorrecta, o usa `append` | No existe |
| **`streaming_design.md`** (Preguntas 1-6) | 30% | Las 6 preguntas respondidas con evidencia concreta de TU pipeline (tu ventana, tu watermark, tus logs) | 3-5 preguntas o respuestas genéricas que repiten teoría sin conectar con tu ejecución | Menos de 3 preguntas |
| **Parte 4 — Kinesis** (opcional, +10% adicional) | +10% | Stream creado, producer y pipeline corriendo contra Kinesis, Pregunta 7 respondida con las 4 partes, recursos limpiados | Parcialmente hecho, o recursos de AWS no limpiados al final | No se hizo (no penaliza — es opcional) |

## Bitácora de delegación

Este lab sigue `../../docs/politica-ia.md`.

| Tarea | ¿Se puede delegar? | Justificación |
|---|---|---|
| Sintaxis de Structured Streaming (dudas puntuales) | Sí | Bajo valor de aprendizaje memorizar sintaxis |
| `kinesis_producer.py` y `crear_stream_kinesis()` (ya dados) | N/A | El objetivo de la Parte 4 es comparar, no reimplementar una decisión ya tomada en el Lab 2a |
| Decidir el tamaño de ventana y el watermark | **No** | Es la decisión de diseño central de la Parte 2 |
| Diseñar la llave del `MERGE` del sink | **No** | Objetivo central de la Parte 3 — conecta directo con la idempotencia del Lab 2a |
| Verificar que el sink no duplica tras varias corridas del productor | **No** | Si no la corriste tú, no tienes evidencia real que citar |
| `streaming_design.md` | **No** | Es el entregable central del lab |

## Troubleshooting

| # | Error / síntoma | Causa probable | Solución |
|---|---|---|---|
| 1 | `streaming_pipeline.py` no encuentra el topic / no recibe nada | El topic `pedidos-ventas` no tiene datos, o el Lab 2a no está corriendo | Verifica `docker ps` (Lab 2a) y corre `productor_kafka.py` antes de arrancar el pipeline |
| 2 | `AnalysisException: Append output mode not supported` (o similar sobre outputMode) | Estás usando `outputMode("append")` con una agregación de ventana sin que el watermark permita finalizarla, o falta el watermark por completo | Revisa que `aplicar_ventana()` tenga `withWatermark()` ANTES del `groupBy` — sin watermark, Spark no sabe cuándo puede considerar "cerrada" una ventana |
| 3 | Filas duplicadas en `ventas_streaming` para la misma ventana+región | La condición del `MERGE` no incluye las 3 columnas (`window_start`, `window_end`, `region`), o usaste `.write.mode("append")` en vez de `merge()` | Revisa `escribir_batch()`: la condición debe comparar las 3 columnas exactamente |
| 4 | El pipeline corre pero `ventas_streaming` nunca se actualiza tras una segunda corrida del productor | Los pedidos nuevos cayeron en una ventana que el watermark YA cerró (si esperaste demasiado entre corridas del productor) | Vuelve a correr el productor poco después de arrancar el pipeline, dentro de tu ventana de watermark |
| 5 | `ClassNotFoundException` o error de conector al usar `STREAM_SOURCE=kinesis` | Falta el `--packages` del conector de Kinesis en el `spark-submit` | Usa el comando completo de la sección 4.4, incluyendo `--packages com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0` |
| 6 | `AccessDeniedException` al listar/crear el stream de Kinesis | Token de AWS Academy expirado (expiran cada 4 horas) o el servicio Kinesis no está habilitado en tu Learner Lab | Renueva las credenciales en el portal de AWS Academy; si el error persiste después de renovar, confirma con el profesor que Kinesis está habilitado en tu curso |
| 7 | El checkpoint da error al reiniciar el pipeline con una configuración distinta (p. ej. cambiaste el tamaño de ventana) | Structured Streaming no permite cambiar la lógica de una consulta y reusar el mismo checkpoint — el checkpoint está atado al plan de ejecución específico | Borra el directorio de `CHECKPOINT_PATH` (`/tmp/lake/checkpoints/lab2b-kafka` por defecto) y arranca de cero — vas a reprocesar todo el topic desde el principio, lo cual es seguro por la idempotencia de tu `MERGE` |
| 8 | `SparkSession` no puede leer/escribir Delta (`Failed to find the data source: delta`) | Falta el paquete `delta-spark` instalado en tu entorno Python | `pip install pyspark delta-spark` — el script ya llama a `configure_spark_with_delta_pip` internamente, no necesitas `--packages` a mano |

## Referencias

- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) — ventanas, watermark, `foreachBatch`.
- [Documentación de Delta Lake](https://docs.delta.io/) — `MERGE INTO` desde `foreachBatch`.
- [Amazon Kinesis Data Streams — Documentación oficial](https://docs.aws.amazon.com/kinesis/)
- `../lab2a-kafka/README.md` — el pipeline base (productor, consumidor manual, garantías de entrega) que este lab extiende.
- Slides de la clase S7 del curso (Structured Streaming, ventanas/watermark, comparativa Kafka vs. Kinesis).
