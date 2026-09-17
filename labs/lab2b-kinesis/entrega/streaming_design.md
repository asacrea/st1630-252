# Diseño de streaming — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

> Cada pregunta trae un bloque **"Evidencia de nuestra ejecución"** con las
> salidas reales del pipeline, citadas textualmente. La respuesta va después
> de ese bloque. Todas las horas están en hora de Colombia (UTC−5).

---

## Pregunta 1 — Ventana y watermark

¿Qué tamaño de ventana elegiste para `aplicar_ventana()`? ¿Qué
watermark usaste? ¿Qué le pasa exactamente a un pedido cuyo
`kafka_time` llega con más retraso que ese watermark respecto al
evento más reciente que Spark ya vio?

**Evidencia de nuestra ejecución**

Configuración en `aplicar_ventana()`:

```python
df.withWatermark("kafka_time", "10 minutes")
  .groupBy(F.window(F.col("kafka_time"), "5 minutes"), F.col("region"))
```

Cada corrida del productor tarda ~20 s (1.000 pedidos, envío síncrono) y el
trigger es de 30 s.

Watermark de cada micro-batch, leído del campo `batchWatermarkMs` de los
archivos `offsets/<batch>` del checkpoint:

| Batch | Trigger | Watermark usado | Resultado del batch |
|---|---|---|---|
| 1 | 18:28:00 | — (aún no había eventos) | 6 filas → tabla creada |
| 2 | 18:28:30 | 18:18:00 | 6 filas → MERGE |
| 3 | 18:29:00 | 18:18:02 | sin cambios |
| 4 | 18:30:30 | 18:18:02 | 6 filas → MERGE |
| 5 | 18:31:00 | 18:20:19 | sin cambios |
| 6 | 18:48:00 | 18:20:19 | 6 filas → MERGE |
| 7 | 18:48:30 | 18:38:00 | 6 filas → MERGE |
| 8 | 18:49:00 | 18:38:06 | sin cambios |
| 9 | 18:50:00 | 18:38:06 | 6 filas → MERGE |
| 10 | 18:50:30 | 18:39:46 | sin cambios |

El watermark de cada batch es el `kafka_time` más reciente visto hasta el
batch anterior, menos 10 minutos. Por ejemplo, el batch 1 terminó con un
evento de las 18:28:00, y el batch 2 usó 18:18:00.

**Prueba con un pedido tardío:**

- Con el pipeline detenido enviamos un pedido (`pedido_id="tardio-1"`,
  `region="LateTest"`) con el timestamp del mensaje de Kafka forzado a
  **18:27:00**, que cae en la ventana 18:25–18:30. El watermark guardado en el
  checkpoint era **18:39:46**, posterior al cierre de esa ventana.
- Al reiniciar, el batch 11 **leyó** el mensaje (el offset de la partición 0
  pasó de 2748 a 2749) y el log mostró `[batch 11] sin cambios`.
- En Silver no hubo commit nuevo (la última versión siguió siendo v5, de las
  18:50:03), hay **0 filas `LateTest`** y la ventana 18:25 sigue con sus 6
  filas y 1.000 pedidos.
- El watermark guardado al terminar el batch 11 siguió en 18:39:46: el evento
  de las 18:27 no lo hizo retroceder.

→ Usamos ventana de 5 minutos y watermark de 10 minutos. Cuando un pedido llega con más retraso que esos 10 minutos respecto al evento más reciente que Spark ya vio, el mensaje sí se lee (el offset avanza y queda en el checkpoint), pero se descarta antes de entrar a la agregación: no crea fila, no lanza error y no deja ningún rastro en Silver. Lo comprobamos mandando un pedido con timestamp forzado a las 18:27 cuando el watermark ya iba en 18:39:46 — el batch lo leyó pero salió "sin cambios", no hubo commit nuevo en Silver y la ventana 18:25 se quedó con sus mismas 6 filas y 1.000 pedidos.

## Pregunta 2 — Checkpoint vs. commit manual (conexión con el Lab 2a)

En el Lab 2a, TÚ decidías cuándo llamar a `consumer.commit()` después
de cada MERGE exitoso. Aquí no hay ningún `commit()` explícito en tu
código. ¿Quién es responsable ahora de "recordar hasta dónde se
procesó"? ¿En qué se parece el `checkpointLocation` al offset commit
del Lab 2a, y en qué se diferencia?

**Evidencia de nuestra ejecución**

Consumer groups registrados en Kafka después de procesar las 4 corridas
(4.000 pedidos):

```
$ docker exec st1630-lab2a-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
(sin salida: no hay ningún consumer group)
```

Contenido del checkpoint `/tmp/lake/checkpoints/lab2b-kafka`:
`commits/`, `metadata`, `offsets/`, `sources/` y `state/`.

`offsets/10` (el `conf` va abreviado):

```
v1
{"batchWatermarkMs":1789342786981,"batchTimestampMs":1789343430003,"conf":{…,"spark.sql.shuffle.partitions":"200"}}
{"pedidos-ventas":{"2":331,"1":1394,"3":527,"0":2748}}
```

`commits/10`:

```
v1
{"nextBatchWatermarkMs":1789342786981}
```

Orden de escritura, con el batch 1 como ejemplo: `offsets/1` se escribió a las
18:28:00, al **inicio** del batch, y `commits/1` a las 18:28:12, cuando el
MERGE ya había terminado.

Tras el reinicio (Pregunta 6), el batch 11 arrancó exactamente en los offsets
de `offsets/10`: leyó 1 mensaje en la partición 0 y ninguno en las demás.

→ Aquí el responsable de recordar hasta dónde se procesó es el checkpoint de Spark, no Kafka; de hecho corrimos kafka-consumer-groups --list después de las 4 corridas y no hay ningún grupo registrado. El checkpointLocation se parece al commit del Lab 2a en que ambos guardan una posición por partición para poder retomar si algo se cae,pero funciona distinto: offsets/N se escribe antes de procesar y commits/N después del MERGE, así que ante una caída Spark reintenta el mismo batch con el mismo rango de offsets (no como en el Lab 2a, donde el siguiente poll() podía traer otro lote). Además guarda también el estado de las ventanas y el watermark.

## Pregunta 3 — outputMode

¿Por qué el pipeline usa `outputMode("update")` en vez de `"complete"`
o `"append"` para este sink en particular? ¿Qué pasaría con cada una
de esas dos alternativas si las usaras aquí?

**Evidencia de nuestra ejecución**

Filas que cada batch con datos le entregó a `escribir_batch()`, frente a las
filas que tenía Silver al terminar ese batch:

| Batch | Filas entregadas al sink | Filas en Silver después |
|---|---:|---:|
| 1 | 6 | 6 |
| 2 | 6 | 6 |
| 4 | 6 | 12 |
| 6 | 6 | 18 |
| 7 | 6 | 18 |
| 9 | 6 | 18 |

En el historial de Delta, `numSourceRows` fue 6 en los cinco MERGE (v1 a v5).

Dato para razonar sobre `append`: una ventana solo se da por cerrada cuando el
watermark supera su fin. La ventana 18:45–18:50 necesita un watermark de
18:50:00 o más, es decir, un evento con `kafka_time` de las 19:00:00 o
posterior. El último watermark que registramos es 18:39:46.

→ Usamos update porque cada batch solo manda las filas que cambiaron (siempre 6), que es lo que el MERGE necesita para ir actualizando una ventana en vivo sin tener que reescribir todo; por eso Silver va creciendo de 6 a 12 a 18 filas según se abren ventanas nuevas, no porque cada batch traiga más datos. Con complete el resultado final sería igual, pero cada batch reescribiría toda la tabla y Spark tendría que guardar el estado de todas las ventanas para siempre. Y con append no habríamos visto nada crecer, porque ahí una ventana solo aparece una vez que el watermark ya pasó su fin.

## Pregunta 4 — La llave del MERGE cambió

En el Lab 2a el `MERGE` usaba `pedido_id` como condición de match. Acá
usas `(window_start, window_end, region)`. ¿Por qué cambió la llave?
¿Sigue siendo idempotente este MERGE de la misma forma que el del Lab
2a? Justifica.

**Evidencia de nuestra ejecución**

Historial de Delta de `/tmp/lake/silver/ventas_streaming`:

| Versión | Hora | Operación | Filas fuente | Insertadas | Actualizadas | Pedidos por ventana después |
|---|---|---|---:|---:|---:|---|
| v0 | 18:28:09 | WRITE | 6 | 6 (escritura inicial) | — | 18:25 → 866 |
| v1 | 18:28:36 | MERGE | 6 | 0 | 6 | 18:25 → 1.000 |
| v2 | 18:30:34 | MERGE | 6 | 6 | 0 | 18:25 → 1.000 · 18:30 → 1.000 |
| v3 | 18:48:04 | MERGE | 6 | 6 | 0 | 18:25 → 1.000 · 18:30 → 1.000 · 18:45 → 584 |
| v4 | 18:48:33 | MERGE | 6 | 0 | 6 | 18:25 → 1.000 · 18:30 → 1.000 · 18:45 → 1.000 |
| v5 | 18:50:03 | MERGE | 6 | 0 | 6 | 18:25 → 1.000 · 18:30 → 1.000 · 18:45 → 2.000 |

El batch 2 leyó solo **134** mensajes (73 + 35 + 9 + 17 por partición), pero
el MERGE dejó las filas de la ventana 18:25 en **1.000** pedidos, no en 134 ni
en 866 + 134 sumados por el sink.

Ventana 18:45–18:50 tras dos corridas seguidas del productor (salidas de
nuestra verificación de Silver):

| Región | Tras corrida A: pedidos / ventas | Tras corrida B: pedidos / ventas |
|---|---|---|
| Barranquilla | 122 / 871,635,400 | 240 / 1,767,838,200 |
| Bogotá | 387 / 3,191,062,100 | 763 / 6,096,145,500 |
| Bucaramanga | 63 / 472,662,000 | 131 / 970,393,200 |
| Cali | 164 / 1,529,400,800 | 333 / 2,720,887,900 |
| Medellín | 203 / 1,448,480,900 | 401 / 2,932,931,000 |
| Otro | 61 / 461,445,400 | 132 / 990,984,500 |

Estado final de Silver: **18 filas, 18 llaves `(window_start, window_end,
region)` distintas, 0 duplicados.**

→ La llave cambió porque acá el grano de la fila ya no es un pedido individual, sino una combinación de ventana y región, entonces pedido_id ni siquiera aparece en el resultado de la agregación, con solo window_start las 6 regiones colisionarían en una fila, y con solo region se mezclarían todas las ventanas. Sigue siendo idempotente, pero de otra forma, en el Lab 2a aplicar el mismo registro dos veces daba la misma fila, mientras que acá Spark ya le entrega al sink el valor acumulado de toda la ventana, no el mensaje individual; entonces el MERGE tiene que reemplazar esa fila con el valor que llega, nunca sumarlo, porque si sumara el resultado quedaría inflado incluso sin ninguna falla de por medio.

## Pregunta 5 — Trigger interval

¿Qué intervalo de trigger usaste (`processingTime`)? ¿Qué trade-off
hay entre un intervalo corto y uno largo para este pipeline en
particular (piensa en el tamaño de tu ventana de la Pregunta 1)?

**Evidencia de nuestra ejecución**

`trigger(processingTime="30 seconds")`, sin cambios respecto al enunciado.

Duración de cada batch, medida entre la escritura de `offsets/<n>` y la de
`commits/<n>`:

| Batch | Trigger | Duración | Resultado |
|---|---|---:|---|
| 0 | 18:09:04 | 7 s | sin cambios (topic vacío, primer batch) |
| 1 | 18:28:00 | 13 s | tabla creada |
| 2 | 18:28:30 | 9 s | MERGE |
| 3 | 18:29:00 | 3 s | sin cambios |
| 4 | 18:30:30 | 5 s | MERGE |
| 5 | 18:31:00 | 2 s | sin cambios |
| 6 | 18:48:00 | 6 s | MERGE |
| 7 | 18:48:30 | 5 s | MERGE |
| 8 | 18:49:00 | 2 s | sin cambios |
| 9 | 18:50:00 | 5 s | MERGE |
| 10 | 18:50:30 | 2 s | sin cambios |

- Los triggers caen en :00 y :30. Después del reinicio, el primer batch
  arrancó de inmediato (18:57:03).
- Dos de las cuatro corridas del productor quedaron partidas entre dos
  batches: la de la ventana 18:25 (866 + 134 mensajes) y la corrida A de la
  ventana 18:45 (584 + 416).
- El estado de la agregación usa 200 particiones (`spark.sql.shuffle.partitions`
  por defecto). El batch 0, sin ningún dato, tardó 7 s.
- Dato para razonar: con una ventana de 5 minutos y un trigger de 30 s, una
  misma ventana puede recibir hasta 10 actualizaciones antes de cerrarse.

→ Usamos trigger de 30 segundos, y con eso una ventana de 5 minutos alcanza a recibir hasta 10 actualizaciones antes de cerrarse. Un trigger más corto daría datos más frescos pero más commits a Delta y batches más chiquitos. Uno más largo bajaría ese costo, pero Silver se atrasaría más y el watermark solo avanza entre batches, así que con un trigger de 5 minutos o más cada ventana casi no alcanzaría a mostrar actualizaciones intermedias, perderíamos la gracia de ver la ventana crecer en vivo.

## Pregunta 6 — Reinicio del job

Si detienes `streaming_pipeline.py` (Ctrl+C) y lo vuelves a correr,
¿reprocesa datos que ya había procesado antes? ¿Por qué es seguro (o
no) que lo haga, dado el sink que implementaste en la Parte 3?

**Evidencia de nuestra ejecución**

**1. Parada.** A las ~18:56, con el pipeline ocioso, le enviamos SIGINT (la
señal de Ctrl+C) al proceso. El manejador de SIGINT de PySpark intentó
cancelar los jobs mientras el hilo principal seguía bloqueado en
`query.awaitTermination()`, y el proceso terminó con:

```
RuntimeError: reentrant call inside <_io.BufferedReader name=3>
...
py4j.protocol.Py4JError: An error occurred while calling o88.awaitTermination
```

Por eso nunca se imprimió el mensaje "Detenido por el usuario (Ctrl+C)" del
script: la excepción que llegó no fue `KeyboardInterrupt`.

**2. Estado del checkpoint al parar.** El último batch (10) tenía tanto
`offsets/10` como `commits/10`: no quedó ningún batch a medias.

**3. Reinicio con el mismo checkpoint.** El topic tenía 4.001 mensajes
disponibles (los 4.000 de las corridas más el pedido tardío). Log del
arranque:

```
Fuente: Kafka (pedidos-ventas @ kafka:29092)
Checkpoint en: /tmp/lake/checkpoints/lab2b-kafka
[batch 11] sin cambios
```

| | Batch 10 (antes de parar) | Batch 11 (después de reiniciar) |
|---|---|---|
| Offsets finales P0, P1, P2, P3 | 2748, 1394, 331, 527 | 2749, 1394, 331, 527 |
| Mensajes leídos por partición | 0, 0, 0, 0 | 1, 0, 0, 0 (el pedido tardío) |
| Watermark usado | 18:39:46 | 18:39:46 |

**4. Silver después del reinicio.** Última versión v5 (18:50:03), 18 filas, 18
llaves distintas. Ventanas: 18:25 → 1.000, 18:30 → 1.000 y 18:45 → 2.000
pedidos.

→ No reprocesó nada: al reiniciar, el batch 11 arrancó justo desde los offsets de offsets/10 (P0 pasó de 2748 a 2749) y solo leyó el mensaje nuevo que había, sin tocar los 4.000 ya procesados. Y aunque hubiera reprocesado algo, sería seguro igual, porque el checkpoint guarda offsets antes de procesar y confirma después del MERGE, así que ante una caída a mitad de proceso Spark repetiría el mismo batch con el mismo rango exacto de offsets.

## Pregunta 7 — Comparativa Kafka vs. Kinesis

**No realizada.** Decidimos no hacer la Parte 4 (Kinesis), que es opcional, así
que no tenemos una ejecución propia contra Kinesis sobre la cual responder esta
comparativa. En la incidencia 4 del `README.md` de esta entrega dejamos
documentado por qué `crear_stream_kinesis()` no funciona con el conector que
indica el enunciado.
