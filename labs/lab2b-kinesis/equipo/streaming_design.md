# Diseño de streaming — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 2026-09-13
**Estudiantes:**
Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co · Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
Sebastián Durán Fernández — sduranf@eafit.edu.co · Daniel Arcila Salazar — darcilas1@eafit.edu.co

> Las horas de este documento están en hora de Colombia. La evidencia completa está en
> `datos/verificacion_sink.md`, donde las ventanas y el historial de Delta aparecen en UTC
> (el reloj del contenedor): 19:50 local = 00:50 UTC.

## Pregunta 1 — Ventana y watermark

¿Qué tamaño de ventana elegiste para `aplicar_ventana()`? ¿Qué
watermark usaste? ¿Qué le pasa exactamente a un pedido cuyo
`kafka_time` llega con más retraso que ese watermark respecto al
evento más reciente que Spark ya vio?

**Ventana de 5 minutos y watermark de 10 minutos** sobre `kafka_time` (el CreateTime que pone
el productor al enviar). Elegimos 5 minutos porque cada corrida del productor cabe entera en
una ventana: la corrida 1 quedó completa en 19:45–19:50 (1.000 pedidos, 6 filas), y eso nos
permitió correrlo dos veces dentro de 19:50–19:55 y ver a Bogotá crecer de 409 a 806. El
watermark de 10 minutos tolera pedidos atrasados sin guardar tanto estado: acepta hasta dos
ventanas de retraso, y a cambio Spark mantiene abiertas unas 3 ventanas × 6 regiones ≈ 18
entradas de estado.

El watermark no usa el reloj de la máquina: al final de cada micro-batch Spark toma el
`kafka_time` más alto que ha visto y le resta 10 minutos, y una ventana se cierra cuando el
watermark supera su `window_end`. Si Spark ya vio pedidos de las 20:06, el watermark queda en
19:56 y la ventana 19:50–19:55 está cerrada. Un pedido de Bogotá con `kafka_time` 19:52 que
llegue en ese momento **se descarta sin aviso**: no genera error, no llega a
`escribir_batch()` y Bogotá se queda en 806. Su offset avanza igual en el checkpoint, así que
no se vuelve a leer. En nuestra prueba no hubo pedidos tardíos: el checkpoint registra
`batchWatermarkMs` = 19:44:12 (el último evento fue a las 19:54:12), así que las dos ventanas
seguían abiertas.

## Pregunta 2 — Checkpoint vs. commit manual (conexión con el Lab 2a)

En el Lab 2a, TÚ decidías cuándo llamar a `consumer.commit()` después
de cada MERGE exitoso. Aquí no hay ningún `commit()` explícito en tu
código. ¿Quién es responsable ahora de "recordar hasta dónde se
procesó"? ¿En qué se parece el `checkpointLocation` al offset commit
del Lab 2a, y en qué se diferencia?

**Ahora lo recuerda Spark, en el `checkpointLocation`** (`/tmp/lake/checkpoints/lab2b-kafka`).
No hacemos commit en Kafka: el progreso no va a `__consumer_offsets` sino a archivos del
checkpoint. Tras las 3 corridas, `offsets/9` registra `{"0":1678, "1":811, "2":205, "3":306}`,
que suma 3.000 pedidos, y en Partición 0 es justo el siguiente al último offset que imprimió
el productor (1677).

**Se parece** al commit manual del Lab 2a en el orden: Spark escribe `offsets/N` con el rango a
leer, ejecuta `escribir_batch()` con el `MERGE` y solo al final escribe `commits/N`. Si el
proceso se cae antes del commit, el batch se reprocesa, así que las dos son **at-least-once**
y el `MERGE` absorbe el reproceso. Spark además reprocesa exactamente el mismo rango, porque
`offsets/N` quedó escrito antes de procesar.

**Se diferencia** en dos cosas. Primero, **el checkpoint guarda estado, no solo offsets**: en
`state/` están los acumulados de las ventanas abiertas y en `offsets/9` el watermark
(19:44:12). Esto es necesario para que nuestro sink sea correcto: como el `MERGE` pisa la fila
con el acumulado, si al reiniciar se recuperaran solo los offsets, Bogotá volvería a contar
desde 0 y el `MERGE` pisaría 806 con un valor menor. En el Lab 2a cada pedido era
independiente y no había nada acumulado que recordar. Segundo, **el checkpoint está atado a la
lógica de la consulta**: el estado se guarda por (ventana de 5 minutos, región), así que si
cambiamos la ventana Spark no puede reutilizarlo y hay que borrar el checkpoint y reprocesar
desde el inicio del topic (seguro gracias al `MERGE`). El offset de un consumer group no
depende de qué haga el código con los mensajes.

## Pregunta 3 — outputMode

¿Por qué el pipeline usa `outputMode("update")` en vez de `"complete"`
o `"append"` para este sink en particular? ¿Qué pasaría con cada una
de esas dos alternativas si las usaras aquí?

**Usamos `update` porque en cada micro-batch Spark entrega solo las filas de ventana que
cambiaron, con su valor acumulado, y el `MERGE` las pisa.** Se ve en el log: en los batches 7 y
8 (corrida 3) llegaron 6 filas, solo las de la ventana 19:50–19:55; las 6 de 19:45 no se
reenviaron aunque la tabla ya tenía 12. Además, `update` respeta el watermark, así que Spark
libera el estado de las ventanas cerradas.

**Con `complete`**, Spark entrega la tabla de resultados entera en cada trigger: el batch 8
habría traído 12 filas en vez de 6. Y como tiene que poder devolver todo, no puede borrar
ventanas viejas aunque haya watermark. En una semana serían 7 × 24 × 12 = 2.016 ventanas × 6
regiones ≈ 12.000 filas reescritas cada 30 s, con el estado creciendo sin límite. El resultado
seguiría siendo correcto (el `MERGE` pisaría con los mismos valores), pero el costo crece para
siempre.

**Con `append`**, Spark emite cada ventana una sola vez, con su valor final, cuando el
watermark la cierra. Al final de nuestra prueba el watermark estaba en 19:44:12 y las dos
ventanas seguían abiertas, así que **la tabla estaría vacía**. Un pedido tardaría como mínimo
~15 minutos en aparecer (5 de ventana + 10 de watermark), y solo si llegan pedidos nuevos que
avancen el watermark: la ventana 19:50–19:55 no se emite hasta ver un pedido posterior a las
20:05. Como cada ventana llega una vez no habría duplicados, pero se pierde la vista casi en
vivo y no se podría verificar el crecimiento de Bogotá de 409 a 806.

## Pregunta 4 — La llave del MERGE cambió

En el Lab 2a el `MERGE` usaba `pedido_id` como condición de match. Acá
usas `(window_start, window_end, region)`. ¿Por qué cambió la llave?
¿Sigue siendo idempotente este MERGE de la misma forma que el del Lab
2a? Justifica.

**La llave cambió porque cambió lo que representa una fila.** En Bronze del Lab 2a cada fila
era un pedido (1.000 filas para 1.000 pedidos), así que `pedido_id` la identificaba. En Silver
cada fila es una ventana+región: tenemos 12 filas para 3.000 pedidos, y Bogotá 19:50–19:55 es
una sola fila que resume 806 pedidos. La columna `pedido_id` ni siquiera existe en esta tabla,
porque la agregación la hace desaparecer. Lo que identifica una fila es (`window_start`,
`window_end`, `region`).

**Sigue siendo idempotente, pero no de la misma forma.** En el Lab 2a el mensaje traía todos
los datos del pedido: reprocesarlo producía exactamente la misma fila, y el `MERGE` bastaba por
sí solo. Aquí el valor que se escribe no sale solo de los mensajes del batch, sino del **estado
acumulado**: el 806 de Bogotá es 409 de la corrida 2 más 397 de la corrida 3. Si Spark se cae
después del `MERGE` del batch 8 pero antes de escribir `commits/8`, al reiniciar reprocesa el
mismo rango de `offsets/8` partiendo del estado del último batch confirmado (el 7), vuelve a
calcular 806 y el `MERGE` pisa 806 con 806. La idempotencia depende de tres piezas juntas: el
`MERGE` que pisa en vez de sumar, el rango de offsets fijo del checkpoint y el estado
versionado por batch. Sin el estado del checkpoint, el reproceso escribiría un valor
incorrecto. Además, en el Lab 2a una fila no debía cambiar nunca, mientras que aquí cambiar es
lo normal: que Bogotá pase de 409 a 806 no es un duplicado sino la ventana creciendo.

## Pregunta 5 — Trigger interval

¿Qué intervalo de trigger usaste (`processingTime`)? ¿Qué trade-off
hay entre un intervalo corto y uno largo para este pipeline en
particular (piensa en el tamaño de tu ventana de la Pregunta 1)?

**Usamos `processingTime="30 seconds"`.** Con ventanas de 5 minutos, eso da unos **10 triggers
por ventana**, suficientes para ver una ventana crecer mientras está abierta. En nuestra
prueba cada corrida del productor quedó repartida en 2 micro-batches, y las corridas 2 y 3
cayeron en batches distintos: por eso vimos a Bogotá en 409 y después en 806 dentro de la misma
ventana 19:50–19:55. En total fueron 6 versiones de Delta para 3 corridas.

**Un trigger corto** (por ejemplo 5 s) da más frescura, pero cada corrida pasaría de 2 a unos 12
batches: 6 veces más `MERGE`, versiones en el `_delta_log` y archivos Parquet pequeños. Con
datos continuos serían 17.280 triggers al día en vez de 2.880, y como cada `MERGE` tiene un
costo fijo (leer la tabla, cruzar y reescribir archivos) sin importar cuántas filas traiga, con
6 filas por batch casi todo el trabajo sería ese costo fijo.

**Un trigger largo** (por ejemplo 5 minutos, igual a la ventana) genera pocas versiones, pero
cada ventana se escribiría una o dos veces mientras está abierta. Las corridas 2 y 3 habrían
caído muy probablemente en el mismo batch: Bogotá habría pasado directo a 806, sin verse
crecer, y la tabla iría hasta una ventana entera de atraso. Los 30 s son un punto medio: la
tabla se ve casi en vivo respecto a la ventana y solo hay un par de versiones por corrida.

## Pregunta 6 — Reinicio del job

Si detienes `streaming_pipeline.py` (Ctrl+C) y lo vuelves a correr,
¿reprocesa datos que ya había procesado antes? ¿Por qué es seguro (o
no) que lo haga, dado el sink que implementaste en la Parte 3?

**Con el checkpoint intacto, no reprocesa.** Lo probamos deteniendo el pipeline con `SIGINT`
dos veces seguidas (la caída fue abrupta: la JVM murió y Python terminó con `Py4JError`, no
con el mensaje de Ctrl+C). Al reiniciar, Spark leyó `commits/9`, vio el último batch confirmado
y retomó desde `offsets/9` (`{"0":1678, "1":811, "2":205, "3":306}`). Como no había mensajes
nuevos, no corrió ningún batch: la tabla siguió en 12 filas y 3.000 pedidos y el historial en
la versión 5. Luego corrimos el productor una vez más, y el batch 10 leyó exactamente los 1.000
pedidos nuevos (P0 de 1678 a 2228, P1 de 811 a 1083, P2 de 205 a 283, P3 de 306 a 406), sin
tocar los 3.000 anteriores. La tabla quedó en 18 filas y 4.000 pedidos.

**Sin checkpoint, sí reprocesa todo, y es seguro gracias al `MERGE`.** Borramos
`/tmp/lake/checkpoints/lab2b-kafka` (el caso de Troubleshooting #7, necesario si cambiamos la
ventana) y reiniciamos. Spark volvió a `startingOffsets="earliest"`, releyó los 4.000 pedidos en
un solo batch (`[batch 0] 18 filas`), recalculó los agregados desde cero y llegó a los mismos
valores. La versión 7 de Delta registra `numTargetRowsUpdated=18`, `numTargetRowsInserted=0`:
el `MERGE` pisó las 18 filas con valores idénticos, así que la tabla quedó en 18 filas, 4.000
pedidos, 0 duplicados, y Bogotá 19:50 en 806. Con un `append`, ese reproceso habría agregado
18 filas repetidas y la suma de pedidos habría pasado a 8.000.

Hay un matiz: el reproceso es seguro porque **todo el topic entró en un solo batch**. Si se
leyera en varios batches (por ejemplo, con `maxOffsetsPerTrigger`), el watermark avanzaría con
los pedidos recientes y podría descartar los de ventanas viejas al reprocesarlas, dejando
valores menores. El `MERGE` evita duplicados, pero no recupera datos que el watermark
descartó.

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

> **Nota:** AWS Academy no nos permitió crear el stream (`AccessDeniedException` en
> `kinesis:CreateStream`, en `us-east-1` y `us-west-2`). Hicimos la Parte 4 contra
> **LocalStack 3.8**, que emula la API de Kinesis en Docker: mismo `kinesis_producer.py`,
> mismo conector de Spark y mismo pipeline. No hay consola, así que en lugar de la captura del
> Data Viewer dejamos la salida de `get-records`. Evidencia completa en
> `datos/verificacion_kinesis.md`.

**(a)** Las dos diferencias que más pesan son el modelo de envío y la serialización.
**Síncrono vs. asíncrono:** en `productor_kafka.py`, `producer.send()` devuelve un *future* y
kafka-python agrupa los mensajes en lotes (`linger_ms=10`) sobre una conexión abierta con el
broker; nosotros forzábamos la espera con `future.get()` para confirmar cada envío. En
`kinesis_producer.py`, `put_record()` es una petición HTTPS que bloquea hasta que Kinesis
responde con `ShardId` y `SequenceNumber`, un registro a la vez. En el benchmark del Lab 2a, el
envío síncrono tardó 16,35 s contra 0,56 s del asíncrono (29,25×): el productor de Kinesis
está, por diseño, del lado lento, y para agrupar hay que cambiar de API a `put_records` (hasta
500 registros por llamada) y armar los lotes a mano. **Serialización:** en Kafka la
configuramos una sola vez al crear el productor (`key_serializer` y `value_serializer`), y cada
`send()` recibe el dict y el string de la región. En Kinesis no hay serializadores: cada llamada
tiene que convertir el pedido con `json.dumps(pedido).encode("utf-8")`, y la `PartitionKey` va
como string aparte. Otra consecuencia: en Kafka la durabilidad es una decisión nuestra
(`acks="all"`), mientras que Kinesis replica en 3 zonas antes de responder y no expone esa
opción.

**(b)** Las cinco equivalencias se cumplen, pero no son idénticas:

| Kafka | Kinesis | Evidencia de nuestros pipelines |
|---|---|---|
| `key` | `PartitionKey` | Misma decisión en ambos productores: la región. |
| partición | shard | Kafka reparte con hash de la key sobre 4 particiones (Bogotá y Cali en P0, 55,6 %); Kinesis con MD5 sobre rangos de hash de 2 shards (todo menos Medellín en el shard 1, 79,4 %). Misma partición caliente por la misma decisión de diseño. |
| offset | `SequenceNumber` | Offsets consecutivos por partición (P0 llegó a 1677); `SequenceNumber` de 56 dígitos, crecientes pero no consecutivos. En el checkpoint de Spark, Kafka guarda "siguiente offset" (`{"0":1678}`) y Kinesis `AFTER_SEQUENCE_NUMBER` con la última secuencia leída por shard. |
| `earliest` | `TRIM_HORIZON` | Ambos pipelines leyeron lo publicado antes de arrancar: Kafka los 4.000 pedidos al reiniciar sin checkpoint, Kinesis la corrida 1 en su batch 0. |
| consumer lag | `IteratorAgeMilliseconds` | Lag de 0 mensajes por partición en el Lab 2a; `MillisBehindLatest` de 1.046.419 ms en `get-records`. |

La diferencia práctica está en las unidades. El lag de Kafka se mide en **mensajes** y hay que
conocer la tasa para saber qué significa (1.000 mensajes son 100 s a 10/s, o 1 s a 1.000/s). El
de Kinesis se mide en **tiempo**: 1.046.419 ms son 17 minutos de atraso, comparables
directamente con nuestra ventana de 5 minutos. A cambio, los offsets consecutivos de Kafka
permiten contar exactamente lo leído (el batch 10 leyó 2228 − 1678 = 550 mensajes de P0), cosa
que no se puede hacer restando `SequenceNumber`.

**(c)** No, mantendríamos Kafka. Con tres criterios de la comparativa de S7:

1. **Portabilidad y dependencia del proveedor.** Kafka corrió en nuestro computador en Docker,
   sin cuenta ni permisos, y el mismo pipeline correría en EMR. Kinesis solo existe en AWS, y
   lo vivimos: AWS Academy nos negó `kinesis:CreateStream` en dos regiones y tuvimos que
   emularlo con LocalStack para poder hacer la parte. Si el datalake depende de Kinesis,
   cualquier restricción de la cuenta o cambio de nube detiene la ingesta.
2. **Retención y replay.** En la Prueba 2 borramos el checkpoint y Spark releyó los 4.000
   pedidos del topic desde `earliest`; el `MERGE` dejó la tabla idéntica. Esa recuperación
   depende de que los datos sigan en la fuente. En Kafka la retención es configurable; nuestro
   stream de Kinesis quedó con 24 horas (el máximo estándar es 7 días), así que un error en la
   lógica descubierto una semana después ya no se podría reprocesar desde Kinesis.
3. **Integración con nuestro stack (Spark + Delta).** El conector de Kafka para Spark sale de
   Maven Central con la misma versión de pyspark, y `configure_spark_with_delta_pip` lo agrega
   solo. Para Kinesis, el paquete que cita el README no era compatible con Spark 3.5 ni con el
   código; tuvimos que descargar un JAR de 81 MB fuera de Maven y rodear un error del conector
   con los endpoints. La ventaja de Kinesis en integración es con servicios de AWS (Lambda,
   Glue, Redshift), que este datalake no usa.

Cambiaríamos de opinión si el equipo no quisiera operar brokers. En el Lab 2a tuvimos que
corregir el `CLUSTER_ID` de KRaft, y con Kinesis no hay broker que mantener. Para un equipo sin
experiencia operando Kafka y un sistema 100 % en AWS, Kinesis (o MSK) reduce ese trabajo. Para
este curso, donde el pipeline tiene que correr igual en local y en EMR, pesa más la
portabilidad.

**(d)** No funcionaría leer Firehose directamente. Kinesis Data Firehose no es un stream que se
pueda consumir: es un servicio de entrega que recibe registros, los agrupa en un búfer (~60 s o
por tamaño) y los deposita en **un único destino** configurado, como S3. No guarda los registros
para lectura, no tiene shards ni `SequenceNumber`, y no ofrece una API tipo `GetRecords`.
Nuestro pipeline depende justamente de eso: el checkpoint de Spark guarda, por shard, desde qué
`SequenceNumber` seguir (`TRIM_HORIZON` en el batch 0, `AFTER_SEQUENCE_NUMBER` después). Con el
nombre de un Firehose, el conector no encontraría ningún Data Stream que listar y la consulta
fallaría al arrancar, antes del primer micro-batch.

La forma correcta es leer **lo que Firehose entrega en S3**, con el file source de Structured
Streaming (`spark.readStream.format("json").load("s3://.../prefijo/")`). Así el checkpoint
registra qué archivos ya se procesaron en lugar de secuencias, la latencia mínima sube a ~60 s
por el búfer (más el descubrimiento de archivos nuevos), y el tiempo de evento debe salir del
campo `timestamp` del JSON, porque ya no existe `approximateArrivalTimestamp`.
`aplicar_ventana()` y `escribir_batch()` seguirían iguales. Si se necesita baja latencia y
además archivar en S3, el patrón de la clase es combinarlos: Data Streams para Spark en tiempo
real y Firehose, alimentado desde ese mismo stream, para guardar el histórico en S3.
