# Diseño de streaming — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026
**Estudiantes:** Hellen Yanes Doria, Sebastian Salazar Henao, Andres Velez Alvarez, Samuel Samper Cardona

## Pregunta 1 — Ventana y watermark

¿Qué tamaño de ventana elegiste para `aplicar_ventana()`? ¿Qué
watermark usaste? ¿Qué le pasa exactamente a un pedido cuyo
`kafka_time` llega con más retraso que ese watermark respecto al
evento más reciente que Spark ya vio?

→ Elegí ventana de 2 minutos y watermark de 3 minutos: la ventana
agrupa los pedidos por `kafka_time` en bloques cortos para dar
visibilidad casi en tiempo real, y el watermark, al ser mayor que la
ventana, da un margen de ~3 minutos de tolerancia a pedidos que
lleguen desordenados o con retraso de red antes de cerrar cada
ventana. Verifiqué esto en la práctica: mis tres corridas del
productor, cada una separada por más de 3 minutos, siempre abrieron
una ventana nueva (`20:14-20:16`, `21:20-21:22`, `21:26-21:28`) en vez
de actualizar la anterior, porque el watermark ya la había cerrado —
la contraparte es que un pedido cuyo `kafka_time` llega con más
retraso que ese watermark respecto al evento más reciente que Spark ya
vio se descarta silenciosamente: no se suma a ninguna ventana, no
genera error, y simplemente se pierde del agregado.

## Pregunta 2 — Checkpoint vs. commit manual (conexión con el Lab 2a)

En el Lab 2a, TÚ decidías cuándo llamar a `consumer.commit()` después
de cada MERGE exitoso. Aquí no hay ningún `commit()` explícito en tu
código. ¿Quién es responsable ahora de "recordar hasta dónde se
procesó"? ¿En qué se parece el `checkpointLocation` al offset commit
del Lab 2a, y en qué se diferencia?

→ En el Lab 2a yo era responsable de recordar el progreso llamando
`consumer.commit()` manualmente después de cada MERGE exitoso,
cubriendo solo el offset del consumidor; aquí no hay ningún commit
explícito porque Spark Structured Streaming delega esa responsabilidad
al `checkpointLocation`, que guarda de forma atómica los offsets de
Kafka leídos, el estado interno del stream (watermark, ventanas
abiertas, agregados) y los metadatos del query. Se parece al commit
del Lab 2a en que ambos permiten reanudar sin perder ni duplicar
trabajo tras un reinicio, pero se diferencia en que el checkpoint es
automático (no una decisión mía de cuándo confirmar) y cubre mucho más
que un offset — protege también el estado de la agregación, lo que
junto con mi MERGE idempotente le da al pipeline garantías de
exactly-once en el sink, algo que el commit manual del Lab 2a no
ofrecía por sí solo.

## Pregunta 3 — outputMode

¿Por qué el pipeline usa `outputMode("update")` en vez de `"complete"`
o `"append"` para este sink en particular? ¿Qué pasaría con cada una
de esas dos alternativas si las usaras aquí?

→ Uso `outputMode("update")` porque mi sink es un MERGE idempotente
por `(window_start, window_end, region)` que solo necesita las filas
que cambiaron en cada micro-batch: con `"complete"` Spark reemitiría
todas las ventanas en cada trigger sin importar si cambiaron, forzando
a reescribir/upsertear toda la tabla cada 30 segundos con un costo de
I/O que crece sin control a medida que se acumulan ventanas; con
`"append"` solo se emitirían ventanas ya definitivamente cerradas por
el watermark, lo que retrasaría la salida y —más grave— impediría
actualizar una ventana ya emitida cuando llega un pedido tardío pero
todavía válido dentro de la tolerancia, que es exactamente el caso que
quiero soportar con mi MERGE.

## Pregunta 4 — La llave del MERGE cambió

En el Lab 2a el `MERGE` usaba `pedido_id` como condición de match. Acá
usas `(window_start, window_end, region)`. ¿Por qué cambió la llave?
¿Sigue siendo idempotente este MERGE de la misma forma que el del Lab
2a? Justifica.

→ En el Lab 2a el MERGE usaba `pedido_id` porque el grano del
resultado era un pedido por fila, una entidad individual con clave
natural única; acá el grano cambió a un agregado por ventana, así que
la clave natural pasó a ser la tupla `(window_start, window_end,
region)`, que identifica de forma única cada fila del resultado
agregado. Sigue siendo idempotente por la misma razón de fondo que en
el Lab 2a —llave única más upsert—: si el mismo micro-batch se
reprocesa (por ejemplo tras un reinicio) o llega un evento tardío
válido que actualiza una ventana ya emitida, el MERGE encuentra la
fila por su llave y la reemplaza con el valor recalculado, que es una
función determinista de los eventos de esa ventana, así que nunca se
duplica ni se corrompe, solo cambia de "agregado de pedidos
individuales" a "agregado de eventos por ventana".

## Pregunta 5 — Trigger interval

¿Qué intervalo de trigger usaste (`processingTime`)? ¿Qué trade-off
hay entre un intervalo corto y uno largo para este pipeline en
particular (piensa en el tamaño de tu ventana de la Pregunta 1)?

→ El trigger que usa el pipeline es `processingTime="30 seconds"`
(definido en el `main()` del script, no es algo que yo eligiera en los
TODO, pero sí lo verifiqué corriéndolo). Con mi ventana de 2 minutos,
un trigger de 30 segundos evalúa cada ventana varias veces durante su
vida útil y la refleja en el sink poco después de cerrarse (baja
latencia), a costa de generar más micro-batches pequeños y más
transacciones MERGE contra Delta; de hecho mi propia corrida mostró el
trade-off en carne propia — un log de la Terminal 2 marcó "the trigger
interval is 30000 milliseconds, but spent 38766 milliseconds", es
decir, un batch se demoró más que el intervalo del trigger, lo que
confirma que un trigger corto exige que cada micro-batch termine
rápido o empieza a acumular atraso; un trigger más largo (p. ej. 2-5
minutos) reduciría esa presión y el número de transacciones, pero
retrasaría la visibilidad de ventanas ya cerradas.

## Pregunta 6 — Reinicio del job

Si detienes `streaming_pipeline.py` (Ctrl+C) y lo vuelves a correr,
¿reprocesa datos que ya había procesado antes? ¿Por qué es seguro (o
no) que lo haga, dado el sink que implementaste en la Parte 3?

→ Si detengo `streaming_pipeline.py` con Ctrl+C y lo vuelvo a correr
apuntando al mismo `CHECKPOINT_PATH`, Spark retoma desde los offsets
guardados en el checkpoint en vez de reprocesar el topic completo,
aunque puede volver a ejecutar el último micro-batch que estaba en
curso si se detuvo antes de que el checkpoint confirmara su
finalización. Esto es seguro precisamente por el MERGE idempotente de
la Parte 3: si ese batch se reejecuta, las filas ya escritas se
encuentran por la llave `(window_start, window_end, region)` y se
sobreescriben con el mismo valor recalculado, sin generar duplicados;
si en cambio borrara el checkpoint, Spark reprocesaría todo el topic
desde `startingOffsets`, lo cual seguiría siendo seguro por la misma
idempotencia, solo que más costoso e innecesario.

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

→ No realizada. No implementé la Parte 4 (Kinesis), por lo que no
puedo responder la comparativa con experiencia real de ambos
producers.
