# Diseño de streaming — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026
**Estudiante:** Sebastian Andres Medina Cabezas

## Pregunta 1 — Ventana y watermark

¿Qué tamaño de ventana elegiste para `aplicar_ventana()`? ¿Qué
watermark usaste? ¿Qué le pasa exactamente a un pedido cuyo
`kafka_time` llega con más retraso que ese watermark respecto al
evento más reciente que Spark ya vio?

→ Usé una ventana de 5 minutos y un watermark de 10 minutos.
La ventana de 5 min agrega las ventas por región en bloques de tiempo
manejables, y el watermark de 10 min (el doble de la ventana) da margen
para tolerar pedidos que lleguen con algo de retraso sin cerrar la
ventana demasiado pronto.

Un pedido cuyo `kafka_time` llega con más de 10 minutos de retraso
respecto al evento más reciente que Spark ya vio se considera un dato
tardío: Spark lo descarta. Concretamente, cuando el
watermark ya avanzó más allá del final de la ventana a la que ese
pedido pertenecería, esa ventana ya fue cerrada y su estado liberado de
memoria. El pedido sí llega al stream y Spark lo lee, pero al no tener
ya la ventana abierta, no lo suma a ningún agregado: no aparece en
`ventas_totales` ni en `num_pedidos` de esa región/ventana.

## Pregunta 2 — Checkpoint vs. commit manual (conexión con el Lab 2a)

En el Lab 2a, TÚ decidías cuándo llamar a `consumer.commit()` después
de cada MERGE exitoso. Aquí no hay ningún `commit()` explícito en tu
código. ¿Quién es responsable ahora de "recordar hasta dónde se
procesó"? ¿En qué se parece el `checkpointLocation` al offset commit
del Lab 2a, y en qué se diferencia?

→ Ahora el responsable es Spark Structured Streaming, a través del
`checkpointLocation` que le pasé al `writeStream`
(`/tmp/lake/checkpoints/lab2b-...`). Yo ya no llamo a ningún `commit()`:
Spark administra el progreso por mí. En cada micro-batch, antes de
procesar, escribe en el checkpoint qué offsets/secuencias va a leer, y
después de que el batch termina bien, marca ese batch como completado.

En qué se parece al commit del Lab 2a: ambos guardan de forma
durable "hasta dónde se procesó", de modo que al reiniciar el proceso
se retoma desde ese punto en vez de desde el principio. Cumplen el
mismo rol conceptual (registrar el avance para no reprocesar todo).

En qué se diferencia:
- En el Lab 2a el commit era manual y decidido por mí (yo elegía
  llamarlo después del MERGE exitoso, para lograr at-least-once). Aquí
  es automático y gestionado por Spark, sin código mío.
- El checkpoint guarda mucho más que un offset: guarda también los
  offsets/secuencias por partición-shard de cada batch y el
  estado de las ventanas con watermark (el estado agregado
  intermedio), no solo un puntero. El commit de Kafka del Lab 2a era
  únicamente el offset del consumer group.
- El checkpoint está acoplado a la query de Spark (su ubicación y
  su esquema de estado), mientras que el offset commit vivía en el
  propio Kafka (en el topic interno `__consumer_offsets`).

## Pregunta 3 — outputMode

¿Por qué el pipeline usa `outputMode("update")` en vez de `"complete"`
o `"append"` para este sink en particular? ¿Qué pasaría con cada una
de esas dos alternativas si las usaras aquí?

→ Uso `outputMode("update")` porque en cada trigger Spark me entrega
solo las filas de ventana que cambiaron desde el último batch, y mi
sink (`foreachBatch` + MERGE) hace un UPSERT: pisa la fila existente de
esa `(window_start, window_end, region)` con el valor actualizado. Esta
combinación es la correcta para agregados por ventana que se van
actualizando conforme llegan más pedidos antes de que el watermark
cierre la ventana.

Si usara `"complete"`: Spark reescribiría en cada trigger la tabla
de resultados completa (todas las ventanas de todas las regiones,
desde el inicio). Funcionaría en términos de correctitud, pero es
ineficiente: la cantidad de datos a reescribir crece sin parar y, de
hecho, `complete` exige mantener todo el estado siempre, lo que anula
el beneficio del watermark de liberar memoria. Para un sink que va
creciendo con el tiempo, es inviable.

Si usara `"append"`: Spark solo emite una fila de ventana cuando
esa ventana ya está cerrada definitivamente (cuando el watermark la
supera), y una sola vez. El problema para mi caso es doble: (1) no
vería resultados parciales/actualizados mientras la ventana sigue
abierta, solo el valor final tras 10 min de watermark; y (2) como cada
ventana se emite una única vez, el UPSERT pierde sentido. `append` sirve
más para sinks tipo insert-only, no para un agregado que se refina en
el tiempo como el mío.

## Pregunta 4 — La llave del MERGE cambió

En el Lab 2a el `MERGE` usaba `pedido_id` como condición de match. Acá
usas `(window_start, window_end, region)`. ¿Por qué cambió la llave?
¿Sigue siendo idempotente este MERGE de la misma forma que el del Lab
2a? Justifica.

→ La llave cambió porque cambió la unidad de dato que estoy
guardando. En el Lab 2a el sink almacenaba pedidos individuales,
así que la identidad natural de cada fila era `pedido_id` (un pedido =
una fila). Aquí ya no guardo pedidos individuales sino agregados por
ventana de tiempo y región: cada fila del sink es "las ventas de la
región X en la ventana [inicio, fin]". Por eso la identidad de una fila
es la combinación `(window_start, window_end, region)`, que es lo que
la hace única. Usar `pedido_id` aquí no tendría sentido porque en el
agregado ya no existen pedidos sueltos.

Sí sigue siendo idempotente, y por la misma razón de fondo que en el
Lab 2a: el MERGE hace match por una llave estable y usa
`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`. Si el mismo
micro-batch se reprocesa (por un reinicio o un reintento), vuelve a
llegar la misma `(window_start, window_end, region)` con su valor
recalculado: el MERGE encuentra la fila existente y la actualiza en
sitio en vez de insertar un duplicado. El resultado final es el mismo
sin importar cuántas veces se aplique.

## Pregunta 5 — Trigger interval

¿Qué intervalo de trigger usaste (`processingTime`)? ¿Qué trade-off
hay entre un intervalo corto y uno largo para este pipeline en
particular (piensa en el tamaño de tu ventana de la Pregunta 1)?

→ Usé `trigger(processingTime="30 seconds")`: Spark dispara un
micro-batch cada 30 segundos.

**Trade-off intervalo corto (p. ej. 5s):** los agregados se actualizan
más seguido, así que veo resultados casi en vivo (menor latencia).
Pero: cada batch procesa menos datos, hay más overhead por batch (más
lecturas al stream, más operaciones MERGE sobre Delta, más archivos
pequeños), y con un sink Delta eso puede degradar el rendimiento. Como
mi ventana es de 5 minutos, un trigger de 5s significaría actualizar
muchísimas veces una misma ventana que igual no se cierra hasta pasados
los 5 min + watermark, o sea mucho trabajo repetido para poca ganancia.

**Trade-off intervalo largo (p. ej. 2 min):** menos overhead, batches
más grandes y eficientes, menos archivos en Delta. Pero mayor latencia:
tardo más en ver reflejadas las ventas recientes.

**Por qué 30s encaja con mi ventana de 5 min:** es un punto intermedio
sensato. Con ventanas de 5 minutos no necesito refrescar cada pocos
segundos (la ventana no cambia tan rápido de "estado interesante"), y
30s me da ~10 actualizaciones por ventana antes de que cierre, que es
suficiente para observar el refinamiento del agregado sin saturar el
sink con micro-batches minúsculos. De hecho, en mi corrida el primer
batch tardó ~49s (leyó todo el backlog con TRIM_HORIZON) y Spark avisó
"batch is falling behind"; eso confirma que el intervalo tiene que ser
holgado frente al costo real de cada batch.

## Pregunta 6 — Reinicio del job

Si detienes `streaming_pipeline.py` (Ctrl+C) y lo vuelves a correr,
¿reprocesa datos que ya había procesado antes? ¿Por qué es seguro (o
no) que lo haga, dado el sink que implementaste en la Parte 3?

→ En condiciones normales **no reprocesa todo desde cero**: gracias al
`checkpointLocation`, al reiniciar Spark lee el último batch completado
y retoma desde ahí, no desde el principio del stream (aunque yo haya
configurado `earliest`/`TRIM_HORIZON`, eso solo aplica la **primera**
vez que no existe checkpoint). Puede reprocesar como mucho el último
micro-batch que quedó a medias si lo interrumpí justo en el medio.

**Y aunque reprocese ese último batch, es seguro**, precisamente por el
sink de la Parte 3. El sink no hace append ciego: hace un **MERGE
idempotente** por `(window_start, window_end, region)`. Si un batch se
vuelve a aplicar, las ventanas que trae hacen match con las filas ya
escritas y las **actualizan en sitio** (whenMatchedUpdateAll) en vez de
duplicarlas. El estado final del sink queda igual sin importar si ese
batch se aplicó una o dos veces.

Esta es justamente la propiedad que hace que el pipeline sea tolerante
a fallos: checkpoint (para no rehacer todo) + sink idempotente (para
que un reintento no ensucie los datos) = semántica **effectively-once**
sobre el resultado final, aun cuando el motor por debajo solo garantice
at-least-once en la lectura.

## Pregunta 7 (solo si hiciste la Parte 4 — Kinesis) — Comparativa Kafka vs. Kinesis

Responde basándote en tu experiencia real implementando ambos:

(a) ¿Qué diferencias observaste en el código del producer? (`boto3
    put_record` vs. `kafka-python producer.send`)
(b) ¿Qué equivalencias encontraste entre los conceptos? (`PartitionKey`
    vs. `key`, `ShardId` vs. partición, `SequenceNumber` vs. offset,
    `TRIM_HORIZON` vs. `earliest`, `IteratorAgeMilliseconds` vs. consumer lag)
(c) Para el datalake del curso (EMR + S3 + Delta Lake), ¿cambiarías
    Kafka por Kinesis en producción? Justifica citando al menos 3
    criterios de la comparativa vista en clase (S7).
(d) ¿Qué pasaría si intentaras leer de Kinesis Data Firehose
    directamente con Spark Structured Streaming, en vez de Kinesis
    Data Streams?

→ Hice la Parte 4. Respuestas basadas en la implementación real:

**(a) Diferencias en el código del producer.**
- **Cliente:** en Kafka creaba un `KafkaProducer(bootstrap_servers=...)`
  con serializadores de key/value configurados una vez. En Kinesis uso
  `boto3.client("kinesis", region_name=...)`; no hay serializadores,
  yo mismo hago `json.dumps(pedido).encode("utf-8")` en cada envío.
- **Envío:** Kafka era `producer.send(TOPIC, key=..., value=...)` que
  devuelve un `future`, y yo hacía `future.get()` para el envío
  síncrono. Kinesis es `kinesis.put_record(StreamName=..., Data=...,
  PartitionKey=...)`, que ya es síncrono y devuelve la respuesta
  directo (con `ShardId` y `SequenceNumber`).
- **Conexión:** Kafka apunta a un `bootstrap_servers` local; Kinesis no
  lleva host, se conecta por región/endpoint de AWS y **depende de
  credenciales** (esto fue lo que más trabajo dio: las credenciales
  temporales del Learner Lab con `aws_session_token`).
- **Decisión de diseño conservada:** en ambos particiono por región
  (`key=region` en Kafka, `PartitionKey=region` en Kinesis) — la misma
  intención de garantizar orden por región.

**(b) Equivalencias de conceptos.**
| Kinesis | Kafka | Rol común |
|---|---|---|
| `PartitionKey` | `key` del mensaje | decide a qué shard/partición va; `hash(key) % N` → misma región, mismo shard/partición → orden por región |
| `ShardId` | partición | unidad de paralelismo y de orden |
| `SequenceNumber` | offset | posición única y creciente de un registro dentro del shard/partición |
| `TRIM_HORIZON` | `earliest` / `auto_offset_reset=earliest` | empezar a leer desde el inicio de lo retenido |
| `LATEST` | `latest` | leer solo lo nuevo desde ahora |
| `IteratorAgeMilliseconds` | consumer lag | qué tan atrás va el consumidor respecto a la punta del stream |

**(c) ¿Cambiaría Kafka por Kinesis en producción para EMR + S3 + Delta?**
Para el datalake del curso (que ya vive en AWS: EMR + S3), **sí me
inclinaría por Kinesis**, citando estos criterios de la comparativa S7:
1. **Operación / gestión:** Kinesis es totalmente administrado (no
   administro brokers, ZooKeeper/KRaft, parches ni escalado de disco),
   mientras que un Kafka propio exige operar el clúster. Con un equipo
   pequeño, menos carga operativa.
2. **Integración con el ecosistema AWS:** al estar todo en AWS (S3, EMR,
   IAM), Kinesis se integra nativamente (IAM para permisos, Firehose
   hacia S3, CloudWatch para métricas), reduciendo pegamento.
3. **Escalado por shards y modelo de costo:** Kinesis escala por shards
   con capacidad on-demand/provisioned y se paga por uso; para el
   volumen del lab (~2 KB/s) es mínimo y no hay que dimensionar
   hardware por adelantado.

**(d) Leer de Kinesis Data Firehose directamente con Spark Structured
Streaming.**
No funcionaría: **Firehose no es una fuente de lectura para
consumidores**. Firehose es un servicio de **entrega** (delivery):
toma datos y los descarga automáticamente a un destino (S3, Redshift,
OpenSearch, etc.), pero no expone shards ni un iterador para que un
consumer lea de él en streaming, como sí hace Kinesis **Data Streams**.
Spark Structured Streaming necesita una fuente con shards/offsets que
pueda posicionar y checkpointar (Data Streams los tiene; Firehose no).
El patrón correcto sería: productor → Kinesis **Data Streams** →
(Spark lee de aquí) **o** Firehose → S3 → Spark lee los archivos de S3
como fuente. Es decir, con Firehose leería **indirectamente desde S3**,
no de Firehose directamente.
