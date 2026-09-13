---
title: streaming_design_template.md

---

# Diseño de streaming — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** _(12/09/2026)_
**Estudiante:**
* Athina Alejandra Cappelleti García (aacappellg@eafit.edu.co)
* David Alejandro Gutiérrez Leal (dagutierrl@eafit.edu.co)
* Emmanuel Álvarez Castrillón (ealvarezc1@eafit.edu.co)
* Ginna Alejandra Valencia Macuace (gavalencim@eafit.edu.co)
* Mariamny Del Valle Ramírez Telles (mvramirezt@eafit.edu.co)

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

Se usó una ventana de 5 minutos (`F.window(F.col("kafka_time"), "5 minutes")`)
y un watermark de 10 minutos (`.withWatermark("kafka_time", "10 minutes")`).
Un pedido cuyo `kafka_time` llega con más de 10 minutos de retraso
respecto al evento más reciente que Spark ya vio es descartado
silenciosamente: Spark ya cerró y liberó el estado de esa ventana, así
que el mensaje tardío no se suma a ningún agregado, ni genera error.

## Pregunta 2 — Checkpoint vs. commit manual (conexión con el Lab 2a)

En el Lab 2a, TÚ decidías cuándo llamar a `consumer.commit()` después
de cada MERGE exitoso. Aquí no hay ningún `commit()` explícito en tu
código. ¿Quién es responsable ahora de "recordar hasta dónde se
procesó"? ¿En qué se parece el `checkpointLocation` al offset commit
del Lab 2a, y en qué se diferencia?

El `checkpointLocation` (`/tmp/lake/checkpoints/lab2b-kafka`) es responsable de recordar el progreso -- Spark lo guarda automáticamente después de cada micro-batch exitoso, sin necesidad de invocarlo explícitamente (a diferencia de `consumer.commit()` en el Lab 2a). Se parece en que ambos marcan "hasta dónde ya se procesó de forma segura" -- se diferencia en que el checkpoint de Structured Streaming también guarda el estado de las ventanas agregadas y el watermark, no solo offsets. 

## Pregunta 3 — outputMode

¿Por qué el pipeline usa `outputMode("update")` en vez de `"complete"`
o `"append"` para este sink en particular? ¿Qué pasaría con cada una
de esas dos alternativas si las usaras aquí?

Se utiliza `"update"` porque solo se necesita las filas de ventana que CAMBIARON desde el último trigger. `"complete"` reescribiría toda la tabla de resultados en cada batch (inviable a medida que crecen las ventanas). `"append"` generaría una fila nueva cada vez que una ventana recibe otro pedido, en vez de actualizar la existente -- rompería el MERGE, que depende de encontrar coincidencias para actualizar, no solo insertar.

## Pregunta 4 — La llave del MERGE cambió

En el Lab 2a el `MERGE` usaba `pedido_id` como condición de match. Acá
usas `(window_start, window_end, region)`. ¿Por qué cambió la llave?
¿Sigue siendo idempotente este MERGE de la misma forma que el del Lab
2a? Justifica.

→ Cambió porque ya no se mergean pedidos individuales (`pedido_id`), sino agregados por ventana+región. Dos micro-batches que reportan la misma `(window_start, window_end, region)` representan la misma ventana en distintos momentos de su evolución deben pisarse (UPDATE), no sumarse. 
Esto se verificó empíricamente: se corrió el productor dos veces con ~20 minutos de diferencia; como cada corrida cayó en una ventana distinta, se obtuvo 12 filas (6 regiones × 2 ventanas) sin ninguna combinación duplicada -- confirma que el MERGE sigue siendo idempotente (evidencia en la pregunta 6)

## Pregunta 5 — Trigger interval

¿Qué intervalo de trigger usaste (`processingTime`)? ¿Qué trade-off
hay entre un intervalo corto y uno largo para este pipeline en
particular (piensa en el tamaño de tu ventana de la Pregunta 1)?

Se usó `processingTime="30 seconds"`. Trade-off: un trigger corto procesa casi en tiempo real pero genera más overhead de coordinación por micro-batch; uno largo agrupa más datos y reduce overhead, pero retrasa la visibilidad de resultados. Con una ventana de 5 minutos, un trigger de 30s permite ver hasta 10 actualizaciones de una misma ventana antes de que el watermark la cierre.

## Pregunta 6 — Reinicio del job

Si detienes `streaming_pipeline.py` (Ctrl+C) y lo vuelves a correr,
¿reprocesa datos que ya había procesado antes? ¿Por qué es seguro (o
no) que lo haga, dado el sink que implementaste en la Parte 3?

Sí, es seguro que se reprocese. Al reiniciar, Spark retoma desde el `checkpointLocation`, pero incluso si se reprocesara una ventana ya escrita, el MERGE con clave `(window_start, window_end, region)` sobreescribiría la fila con el mismo valor, sin duplicar.

**Evidencia:**

Las imagenes se encuentran en la carpeta de datos

Se corrió el productor dos veces (21:35-21:40 y 21:55-22:00), o sea, se publicaron dos veces los 1000 pedidos con el streaming activo todo el tiempo. El resultado:
12 filas totales -- 6 regiones × 2 ventanas -- sin ninguna fila duplicada para la misma combinación ventana+región. 
Los `num_pedidos`por región en cada ventana coinciden exactamente con el resumen
impreso por `productor_kafka.py` en cada corrida (ej. Bogotá=404 en la primera corrida, Bogotá=385 en la segunda), confirmando que no hubo pérdida ni duplicación de pedidos entre Kafka y la tabla Silver.

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

No realizada
