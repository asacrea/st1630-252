# Diseño de streaming — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Copia este archivo a tu carpeta de entrega como `streaming_design.md`
> y complétalo después de tener `streaming_pipeline.py` corriendo
> contra Kafka. La Pregunta 7 solo aplica si hiciste la Parte 4
> (Kinesis, opcional) — si no la hiciste, elimínala o márcala como
> "no realizada" en vez de dejarla en blanco sin explicación.

## Pregunta 1 — Ventana y watermark

¿Qué tamaño de ventana elegiste para `aplicar_ventana()`? ¿Qué
watermark usaste? ¿Qué le pasa exactamente a un pedido cuyo
`kafka_time` llega con más retraso que ese watermark respecto al
evento más reciente que Spark ya vio?

→ [tu respuesta aquí]

## Pregunta 2 — Checkpoint vs. commit manual (conexión con el Lab 2a)

En el Lab 2a, TÚ decidías cuándo llamar a `consumer.commit()` después
de cada MERGE exitoso. Aquí no hay ningún `commit()` explícito en tu
código. ¿Quién es responsable ahora de "recordar hasta dónde se
procesó"? ¿En qué se parece el `checkpointLocation` al offset commit
del Lab 2a, y en qué se diferencia?

→ [tu respuesta aquí]

## Pregunta 3 — outputMode

¿Por qué el pipeline usa `outputMode("update")` en vez de `"complete"`
o `"append"` para este sink en particular? ¿Qué pasaría con cada una
de esas dos alternativas si las usaras aquí?

→ [tu respuesta aquí]

## Pregunta 4 — La llave del MERGE cambió

En el Lab 2a el `MERGE` usaba `pedido_id` como condición de match. Acá
usas `(window_start, window_end, region)`. ¿Por qué cambió la llave?
¿Sigue siendo idempotente este MERGE de la misma forma que el del Lab
2a? Justifica.

→ [tu respuesta aquí]

## Pregunta 5 — Trigger interval

¿Qué intervalo de trigger usaste (`processingTime`)? ¿Qué trade-off
hay entre un intervalo corto y uno largo para este pipeline en
particular (piensa en el tamaño de tu ventana de la Pregunta 1)?

→ [tu respuesta aquí]

## Pregunta 6 — Reinicio del job

Si detienes `streaming_pipeline.py` (Ctrl+C) y lo vuelves a correr,
¿reprocesa datos que ya había procesado antes? ¿Por qué es seguro (o
no) que lo haga, dado el sink que implementaste en la Parte 3?

→ [tu respuesta aquí]

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

→ [tu respuesta aquí]
