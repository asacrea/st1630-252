# Diseño de streaming — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026  
**Equipo:** Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`), Mateo Sanz Medina (`msanzm@eafit.edu.co`), Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

---

## Pregunta 1 — Ventana y watermark

¿Qué tamaño de ventana elegiste para `aplicar_ventana()`? ¿Qué watermark usaste? ¿Qué le pasa exactamente a un pedido cuyo `kafka_time` llega con más retraso que ese watermark respecto al evento más reciente que Spark ya vio?

→ **Configuración seleccionada:**
- **Tamaño de ventana:** Ventana tumbling de **5 minutos** (`5 minutes`).
- **Watermark:** Umbral de tolerancia de **10 minutos** (`withWatermark("kafka_time", "10 minutes")`).

**Comportamiento ante eventos tardíos (*Late Data*):**  
Spark Structured Streaming mantiene internamente un puntero de marca de agua dinámico calculado como:
$$\text{Watermark} = \max(\text{kafka\_time visto}) - 10\text{ minutos}$$

Cuando llega un pedido rezagado con marca temporal $t_{\text{pedido}}$, Spark evalúa si:
$$t_{\text{pedido}} < \text{Watermark}$$

Si esta condición se cumple, Spark determina que la ventana de tiempo a la que correspondía el pedido ya superó el umbral de tolerancia y su estado ha sido purgado de la memoria RAM (*State Store*). Por lo tanto, el evento es **descartado silenciosamente** (*dropped*): no se suma a `ventas_totales`, no incrementa `num_pedidos` y no se emite ninguna actualización hacia Delta Lake. Esta poda de estado previene fugas de memoria (*OutOfMemoryError*) al procesar streams de ejecución indefinida.

---

## Pregunta 2 — Checkpoint vs. commit manual (conexión con el Lab 2a)

En el Lab 2a, TÚ decidías cuándo llamar a `consumer.commit()` después de cada MERGE exitoso. Aquí no hay ningún `commit()` explícito en tu código. ¿Quién es responsable ahora de "recordar hasta dónde se procesó"? ¿En qué se parece el `checkpointLocation` al offset commit del Lab 2a, y en qué se diferencia?

→ **Responsable de la persistencia de progreso:**  
El motor de **Spark Structured Streaming** es el único responsable de registrar el progreso mediante el parámetro `.option("checkpointLocation", CHECKPOINT_PATH)`.

**Comparativa entre `checkpointLocation` y el commit del Lab 2a:**

1. **Semejanzas:**
   - Ambos mecanismos garantizan la semántica de entrega *at-least-once*, almacenando los punteros de lectura (partición y offset de Kafka) para evitar re-procesar todo el topic desde el inicio tras una detención o caída.

2. **Diferencias:**
   - **Ubicación del estado:** En el Lab 2a, el commit se enviaba al topic interno de Kafka `__consumer_offsets`. En el Lab 2b, el checkpoint se almacena en el sistema de archivos del datalake (local o S3) bajo directorios transaccionales `/offsets` y `/commits`.
   - **Riqueza del estado:** El commit de Kafka únicamente persiste un par de números enteros (`partition: offset`). El checkpoint de Spark es un log transaccional *write-ahead* (WAL) que almacena no solo los offsets de inicio y fin de cada micro-batch, sino también el **estado en memoria de las ventanas abiertas**, el valor actual del watermark y los identificadores únicos de micro-batch para garantizar transaccionalidad ACID.
   - **Gobernanza:** En el Lab 2a, un error humano al ubicar `consumer.commit()` podía provocar pérdida de datos (*at-most-once* accidental). En Structured Streaming, Spark garantiza que el commit se escriba en el checkpoint **estrictamente después** de que la función `escribir_batch` finaliza de manera exitosa.

---

## Pregunta 3 — outputMode

¿Por qué el pipeline usa `outputMode("update")` en vez de `"complete"` o `"append"` para este sink en particular? ¿Qué pasaría con cada una de esas dos alternativas si las usaras aquí?

→ **Justificación de `outputMode("update")`:**  
El modo `update` es el único adecuado para un sink relacional/Delta Lake que opera mediante UPSERT (`MERGE INTO`). Bajo este modo, Spark entrega en cada micro-batch **únicamente las filas correspondientes a ventanas cuyos acumulados (`ventas_totales`, `num_pedidos`) hayan cambiado** en los últimos 30 segundos debido a la llegada de nuevos pedidos.

**Comportamiento de las alternativas:**
- **Si se usara `complete`:** En cada micro-batch de 30 segundos, Spark emitiría **la totalidad de las ventanas históricas existentes desde el inicio del streaming**, incluso aquellas cerradas hace horas o días. A medida que pasa el tiempo, el volumen del micro-batch crecería sin control, provocando un impacto severo de I/O y degradando la escritura en Delta Lake por recalcular innecesariamente datos inmutables.
- **Si se usara `append`:** En consultas de agregación con watermark, `append` **bloquea la emisión de datos hasta que la ventana se cierra definitivamente** (es decir, cuando el watermark supera `window_end`). Si usáramos `append`:
  1. Perderíamos la visibilidad en tiempo real: los registros tardarían $5\text{ min (ventana)} + 10\text{ min (watermark)} = 15\text{ minutos}$ en aparecer en Delta Lake.
  2. No se reflejarían las ventas intermedias mientras la ventana está abierta.
  3. Si no existiera watermark, Spark abortaría inmediatamente la ejecución con una excepción: `AnalysisException: Append output mode not supported for streaming aggregations without watermark`.

---

## Pregunta 4 — La llave del MERGE cambió

En el Lab 2a el `MERGE` usaba `pedido_id` como condición de match. Acá usas `(window_start, window_end, region)`. ¿Por qué cambió la llave? ¿Sigue siendo idempotente este MERGE de la misma forma que el del Lab 2a? Justifica.

→ **Por qué cambió la clave:**  
En el Lab 2a estábamos ingiriendo la capa **Bronze**, cuyo grano de información es el evento atómico individual de compra (1 fila = 1 pedido); por tanto, la clave natural de unicidad era `pedido_id`.  
En el Lab 2b estamos materializando una vista agregada en la capa **Silver**, cuyo grano analítico es el consolidado de ventas por región geográfica en un intervalo de tiempo específico. La identidad unívoca de un registro en esta tabla es la tupla temporal-espacial:
$$\text{Clave Primaria Lógica} = (\text{window\_start}, \text{window\_end}, \text{region})$$

→ **Garantía de Idempotencia:**  
**SÍ, el `MERGE` es 100% idempotente.** La condición evaluada en Delta Lake es:
```sql
existente.window_start = nuevo.window_start AND 
existente.window_end = nuevo.window_end AND 
existente.region = nuevo.region
```
Cuando nuevos eventos impactan una ventana abierta que ya fue emitida en un micro-batch anterior, la cláusula `WHEN MATCHED THEN UPDATE SET *` **reemplaza** los campos numéricos con el acumulado más reciente en lugar de duplicar registros. Si un micro-batch se procesa dos o más veces (por reintentos tras una falla antes de confirmar el checkpoint), el `MERGE` sobre-escribe exactamente los mismos valores sobre la misma clave, garantizando que el estado final del datalake sea invariable:
$$f(f(\text{datalake})) = f(\text{datalake})$$

---

## Pregunta 5 — Trigger interval

¿Qué intervalo de trigger usaste (`processingTime`)? ¿Qué trade-off hay entre un intervalo corto y uno largo para este pipeline en particular (piensa en el tamaño de tu ventana de la Pregunta 1)?

→ **Intervalo configurado:**  
`.trigger(processingTime="30 seconds")`

→ **Trade-off técnico:**
- **Trigger muy corto (ej. 1s - 5s):** Reduce la latencia a mínimos segundos, pero sobrecarga al coordinador de Spark con una planificación constante de tareas y genera una proliferación de archivos muy pequeños en Delta Lake (*Small Files Problem*), lo cual penaliza las consultas posteriores en Athena o Spark.
- **Trigger muy largo (ej. 5m - 10m):** Genera archivos Parquet más grandes y optimiza el throughput de compresión, pero destruye la propuesta de valor de un pipeline de streaming continuo al degradar la latencia a niveles de un procesamiento batch.
- **Equilibrio de 30 segundos:** Para una ventana analítica de **5 minutos**, un trigger de 30 segundos permite observar hasta 10 actualizaciones dinámicas por ventana, ofreciendo una experiencia en tiempo real óptima para tableros de control sin sobrecargar las transacciones ACID de Delta Lake.

---

## Pregunta 6 — Reinicio del job

Si detienes `streaming_pipeline.py` (Ctrl+C) y lo vuelves a correr, ¿reprocesa datos que ya había procesado antes? ¿Por qué es seguro (o no) que lo haga, dado el sink que implementaste en la Parte 3?

→ **Comportamiento al reiniciar:**  
1. Al reiniciar el script apuntando al mismo `CHECKPOINT_PATH`, Spark consulta el archivo más reciente en el subdirectorio `/commits` para identificar el último micro-batch completado.
2. Si el proceso fue interrumpido de forma limpia entre triggers, Spark **NO reprocesa** eventos anteriores y continúa la lectura exactamente desde los offsets confirmados.
3. Si el proceso fue abortado **a mitad de la ejecución de un micro-batch**, Spark reanuda leyendo nuevamente los offsets correspondientes a ese micro-batch incompleto.

→ **Por qué es seguro:**  
Es **100% seguro** gracias a la conjunción del log transaccional ACID de Delta Lake y el patrón idempotente `foreachBatch + MERGE`. Al re-ejecutar el micro-batch, la condición sobre `(window_start, window_end, region)` actualiza los registros ya existentes sin sumar métricas dos veces ni generar filas redundantes, logrando semántica de procesamiento *exactly-once* de extremo a extremo.

---

## Pregunta 7 (Parte 4 — Kinesis) — Comparativa Kafka vs. Kinesis

### (a) Diferencias en el código del productor:
- **`kafka-python` (`producer.send`):** Opera bajo una conexión TCP persistente hacia los brokers de Kafka (`bootstrap_servers`). El envío es asíncrono y se gestiona mediante colas internas en memoria gobernadas por `linger_ms` y `batch_size`.
- **`boto3` (`kinesis.put_record`):** Opera mediante peticiones HTTPS REST individuales contra el endpoint de AWS Kinesis, requiriendo autenticación con credenciales SigV4 de AWS IAM. Para alto volumen se utiliza `put_records` (batching en HTTP) en lugar de llamadas síncronas individuales.

### (b) Equivalencias de conceptos:
| Apache Kafka | Amazon Kinesis Data Streams | Función Arquitectónica |
| :--- | :--- | :--- |
| `key` de mensaje | `PartitionKey` | Clave usada para aplicar hashing y determinar el destino |
| `Partition` (Partición) | `Shard` | Unidad base de paralelismo, throughput y capacidad de I/O |
| `Offset` | `SequenceNumber` | Puntero monótono creciente que identifica el evento dentro de la partición/shard |
| `auto_offset_reset="earliest"` | `startingposition="TRIM_HORIZON"` | Posición inicial de lectura desde el dato más antiguo retenido |
| `Consumer Lag` | `IteratorAgeMilliseconds` | Tiempo transcurrido entre el evento más reciente y la lectura actual |

### (c) Elección para el Data Lake del curso (EMR + S3 + Delta Lake):
Para el datalake del curso desplegado en AWS, **Kinesis Data Streams** representa una opción más conveniente por las siguientes razones:
1. **Modelo Serverless y Operabilidad:** Kinesis es un servicio administrado nativo que elimina la sobrecarga operativa de aprovisionar instancias EC2, configurar listeners en Docker/KRaft y gestionar el espacio en disco de brokers de Kafka.
2. **Seguridad e Integración Nativa:** Kinesis se integra sin fricción con roles de IAM en AWS Academy/EMR, políticas de mínimo privilegio y cifrado en reposo con AWS KMS.
3. **Escalabilidad por Shards:** El escalado se ajusta agregando shards (`UpdateShardCount`) con facturación transparente por shard-hora, a diferencia de Kafka donde reasignar particiones exige rebalancear manualmente el almacenamiento entre brokers.

### (d) Lectura directa desde Kinesis Data Firehose con Spark Structured Streaming:
**NO es posible técnicamente.** Kinesis Data Firehose es un servicio de entrega unidireccional (*push-based ingestion*) diseñado para cargar datos de manera continua en sinks finales como Amazon S3, Redshift u OpenSearch. Firehose no expone una API de lectura de streams (no provee `GetRecords` ni cursores `ShardIterator`). Para leer con Spark Structured Streaming, el origen debe ser obligatoriamente un **Kinesis Data Stream** (o bien consumir los micro-lotes de archivos Parquet/JSON que Firehose deposita en Amazon S3 usando Auto Loader/Structured Streaming sobre archivos).
