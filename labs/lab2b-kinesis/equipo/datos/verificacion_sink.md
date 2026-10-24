# Verificación del sink idempotente (Parte 3)

**Fecha:** 2026-09-13 (19:45–20:21 hora Colombia = 00:45–01:21 UTC del 2026-09-14)
**Configuración:** ventana de 5 minutos, watermark de 10 minutos, trigger cada 30 s,
`outputMode("update")`, `MERGE` por `(window_start, window_end, region)`.

Las horas de las ventanas y del historial de Delta están en **UTC**, que es el reloj del
contenedor. Restar 5 horas para la hora local.

## Qué se quería probar

1. Que una ventana que recibe pedidos en **dos micro-batches** queda como **una sola fila**
   por región, con el valor acumulado, y no como dos filas.
2. Que una segunda corrida del productor dentro de la **misma ventana** hace **crecer** las
   filas existentes, sin crear filas nuevas para esa ventana.
3. Que después de varias corridas no hay ninguna combinación ventana+región repetida.

## Cómo se corrió

Todo en contenedores sobre la red del Lab 2a (`lab2a-kafka_default`), con la imagen
`st1630-lab2a-spark` (PySpark 3.5.6, delta-spark 3.3.0, Java 17).

```bash
# Pipeline (desde labs/lab2b-kinesis/)
docker run -d --name lab2b-stream --network lab2a-kafka_default \
  -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake -v lab2a-ivy:/root/.ivy2 \
  -e KAFKA_BOOTSTRAP=kafka:29092 st1630-lab2a-spark python -u /scripts/streaming_pipeline.py

# Productor, 3 veces (desde labs/lab2a-kafka/)
docker run --rm --network lab2a-kafka_default -v "${PWD}/equipo/scripts:/scripts" \
  -e KAFKA_BOOTSTRAP=kafka:29092 st1630-lab2a-spark python -u /scripts/productor_kafka.py

# Lectura de la tabla (desde labs/lab2b-kinesis/)
docker run --rm -v "${PWD}/equipo/scripts:/scripts" -v lab2b-lake:/tmp/lake \
  -v lab2a-ivy:/root/.ivy2 st1630-lab2a-spark python /scripts/leer_ventanas.py "despues de la corrida 3"
```

El topic `pedidos-ventas` estaba vacío al arrancar el pipeline (4 particiones, RF 1).

## Las tres corridas del productor

Cada corrida publica 1.000 pedidos. Resumen por región que imprimió el productor:

| Región | Corrida 1 (~19:48) | Corrida 2 (~19:50) | Corrida 3 (~19:53) |
|---|---|---|---|
| Bogotá | 429 | 409 | 397 |
| Medellín | 176 | 189 | 211 |
| Cali | 153 | 151 | 139 |
| Barranquilla | 101 | 95 | 110 |
| Bucaramanga | 80 | 79 | 76 |
| Otro | 61 | 77 | 67 |
| **Total** | **1.000** | **1.000** | **1.000** |

La corrida 1 cayó en la ventana 00:45–00:50 UTC. Las corridas 2 y 3 cayeron en la ventana
00:50–00:55 UTC.

## Log del pipeline

`docker logs -t lab2b-stream`, hora UTC de cada `print` de `escribir_batch()`:

```
00:45:50  [batch 0] 0 filas de ventana actualizadas   <- topic vacío
00:49:04  [batch 1] 6 filas de ventana actualizadas   <- corrida 1, primera parte
00:49:35  [batch 2] 6 filas de ventana actualizadas   <- corrida 1, resto
00:50:05  [batch 3] 0 filas de ventana actualizadas
00:51:02  [batch 4] 6 filas de ventana actualizadas   <- corrida 2, primera parte
00:51:36  [batch 5] 6 filas de ventana actualizadas   <- corrida 2, resto
00:52:02  [batch 6] 0 filas de ventana actualizadas
00:54:02  [batch 7] 6 filas de ventana actualizadas   <- corrida 3, primera parte
00:54:34  [batch 8] 6 filas de ventana actualizadas   <- corrida 3, resto
00:55:07  [batch 9] 0 filas de ventana actualizadas
```

Cada corrida del productor tardó más que un trigger de 30 s, así que quedó repartida en **dos
micro-batches seguidos**. En el segundo, Spark volvió a entregar **las mismas 6
ventanas+región** con más pedidos. Ese es justo el caso para el que existe el `MERGE`.

## Historial de la tabla Delta

| Versión | Timestamp (UTC) | Operación | Micro-batch |
|---|---|---|---|
| 0 | 00:49:12 | WRITE | batch 1: la tabla no existía, se crea con `overwrite` |
| 1 | 00:49:44 | MERGE | batch 2 |
| 2 | 00:51:10 | MERGE | batch 4 |
| 3 | 00:51:45 | MERGE | batch 5 |
| 4 | 00:54:09 | MERGE | batch 7 |
| 5 | 00:54:44 | MERGE | batch 8 |

Los batches con 0 filas no generan versión: `escribir_batch()` sale antes de escribir.

## Resultado 1: una ventana en dos micro-batches queda en una fila

Leída después de la corrida 1 (versión 1):

```
+-------------------+-------------------+------------+--------------+-----------+
|window_start       |window_end         |region      |ventas_totales|num_pedidos|
+-------------------+-------------------+------------+--------------+-----------+
|2026-09-14 00:45:00|2026-09-14 00:50:00|Barranquilla|6.785649E8    |101        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Bogotá      |3.3332415E9   |429        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Bucaramanga |6.780488E8    |80         |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Cali        |1.0803835E9   |153        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Medellín    |1.2543169E9   |176        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Otro        |4.195183E8    |61         |
+-------------------+-------------------+------------+--------------+-----------+
filas: 6 | suma num_pedidos: 1000
```

Los batches 1 y 2 trajeron las mismas 6 ventanas+región, y la tabla quedó con **6 filas**,
no 12. Los `num_pedidos` coinciden exactamente con el resumen de la corrida 1.

## Resultado 2: la misma ventana crece con la corrida 3

Leída después de la corrida 2 (versión 3), la ventana 00:50–00:55 tenía los valores de la
corrida 2: Bogotá 409, Medellín 189, Cali 151, Barranquilla 95, Bucaramanga 79, Otro 77
(12 filas en total, 2.000 pedidos, 0 duplicados).

Después de la corrida 3 (versión 5), salida de `leer_ventanas.py`:

```
=== VENTANAS SILVER [despues de la corrida 3] ===
filas_totales             = 12
suma_num_pedidos          = 3000
duplicados_ventana_region = 0

+-------------------+-------------------+------------+--------------+-----------+
|window_start       |window_end         |region      |ventas_totales|num_pedidos|
+-------------------+-------------------+------------+--------------+-----------+
|2026-09-14 00:45:00|2026-09-14 00:50:00|Barranquilla|6.785649E8    |101        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Bogotá      |3.3332415E9   |429        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Bucaramanga |6.780488E8    |80         |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Cali        |1.0803835E9   |153        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Medellín    |1.2543169E9   |176        |
|2026-09-14 00:45:00|2026-09-14 00:50:00|Otro        |4.195183E8    |61         |
|2026-09-14 00:50:00|2026-09-14 00:55:00|Barranquilla|1.595026E9    |205        |
|2026-09-14 00:50:00|2026-09-14 00:55:00|Bogotá      |6.3173926E9   |806        |
|2026-09-14 00:50:00|2026-09-14 00:55:00|Bucaramanga |1.2490026E9   |155        |
|2026-09-14 00:50:00|2026-09-14 00:55:00|Cali        |2.3359694E9   |290        |
|2026-09-14 00:50:00|2026-09-14 00:55:00|Medellín    |3.0488791E9   |400        |
|2026-09-14 00:50:00|2026-09-14 00:55:00|Otro        |1.1707195E9   |144        |
+-------------------+-------------------+------------+--------------+-----------+
```

Crecimiento de la ventana 00:50–00:55:

| Región | Corrida 2 | + Corrida 3 | = En Delta |
|---|---|---|---|
| Bogotá | 409 | 397 | **806** |
| Medellín | 189 | 211 | **400** |
| Cali | 151 | 139 | **290** |
| Barranquilla | 95 | 110 | **205** |
| Bucaramanga | 79 | 76 | **155** |
| Otro | 77 | 67 | **144** |

Las 6 filas de la ventana 00:45–00:50 no cambiaron entre la versión 1 y la 5.

## Qué demuestra y por qué funciona

- **La tabla pasó de 6 a 12 filas y ahí se quedó**, aunque hubo 6 micro-batches con datos
  (36 filas de ventana entregadas en total). Con `append` habría 36 filas, con varias
  versiones de la misma ventana+región y valores distintos: cualquier suma sobre la tabla
  contaría los pedidos más de una vez.
- **`whenMatchedUpdateAll` pisa, no suma.** Con `outputMode("update")`, Spark no entrega el
  incremento del micro-batch sino el **agregado acumulado** que guarda en su estado para
  esa ventana. Por eso reemplazar la fila deja el valor correcto (806 en Bogotá). Si el
  `MERGE` sumara `existente + nuevo`, contaría dos veces lo ya acumulado.
- **La llave tiene que incluir la ventana.** Con solo `region`, la corrida 2 habría pisado
  las filas de la ventana 00:45–00:50 y se habría perdido ese histórico: la tabla tendría
  6 filas en vez de 12.
- **Watermark:** el checkpoint registra `batchWatermarkMs` = 00:44:12 UTC en `offsets/9` (el
  evento más reciente, de la corrida 3, fue a las 00:54:12). Las dos ventanas seguían
  **abiertas** al terminar la corrida 3. La ventana 00:50–00:55 se cierra cuando lleguen
  eventos posteriores a las 01:05 UTC (fin de la ventana + 10 min), cosa que ocurrió con la
  corrida 4 (ver abajo). Un pedido que llegue después con `kafka_time` dentro de esa ventana
  se descarta.

---

# Pruebas de reinicio (Pregunta 6)

Hechas a continuación de lo anterior, con el mismo topic, tabla y volumen `lab2b-lake`.

## Prueba 1: reinicio con el checkpoint intacto

**Qué se hizo.** A las 01:12 UTC, con el pipeline inactivo desde el batch 9 (00:55), se detuvo
el contenedor con la misma señal que manda Ctrl+C y se volvió a arrancar. Se hizo **dos veces
seguidas** (01:12:29 y 01:13:48 UTC).

```bash
docker kill --signal=SIGINT lab2b-stream
docker start lab2b-stream
```

**Cómo se cayó.** No fue una salida limpia: la señal le llega también a la JVM de Spark dentro
del contenedor. El log muestra el manejador de PySpark intentando `cancelAllJobs()` mientras la
JVM ya moría, y el proceso terminó con `Exited (1)`:

```
py4j.protocol.Py4JNetworkError: Error while sending or receiving
py4j.protocol.Py4JError: An error occurred while calling o88.awaitTermination
```

No aparece el mensaje `Detenido por el usuario (Ctrl+C).` del script. En la práctica es una
caída abrupta.

**Qué pasó al reiniciar.**

- El pipeline arrancó, imprimió `Fuente` y `Checkpoint en` y **no corrió ningún batch**. No
  hubo batch 10 ni reproceso de los batches 0–9.
- El checkpoint siguió en `offsets/9` y `commits/9`, y no se creó `offsets/10`.
- `leer_ventanas.py "tras reinicio"`: `filas_totales = 12`, `suma_num_pedidos = 3000`,
  `duplicados_ventana_region = 0`, historial en la versión 5.

Spark lee `commits/9`, ve que ese batch terminó y retoma desde `offsets/9`. Sin mensajes nuevos
en Kafka no hay nada que procesar, así que no crea micro-batches. Los batches vacíos del log
original (3, 6 y 9) no son "cada 30 s": son batches sin datos que Spark corre después de
recibir pedidos, para avanzar el watermark.

## Prueba 1b: corrida 4 del productor después del reinicio

Resumen de la corrida 4 (~01:17 UTC), leído de la tabla: Bogotá 407, Medellín 197, Cali 143,
Barranquilla 100, Otro 78, Bucaramanga 75 = **1.000**.

Log:

```
01:17:38  [batch 10] 6 filas de ventana actualizadas
          [batch 11] 0 filas de ventana actualizadas
```

Offsets leídos por el batch 10, comparando `offsets/9` con `offsets/10`:

| Partición | `offsets/9` | `offsets/10` | Leídos |
|---|---|---|---|
| P0 | 1678 | 2228 | 550 |
| P1 | 811 | 1083 | 272 |
| P2 | 205 | 283 | 78 |
| P3 | 306 | 406 | 100 |
| **Total** | | | **1.000** |

El batch 10 empezó exactamente donde quedó el batch 9 antes de las dos caídas y leyó solo los
1.000 pedidos nuevos. Tabla después de la corrida 4 (versión 6):

```
filas_totales             = 18
suma_num_pedidos          = 4000
duplicados_ventana_region = 0

|2026-09-14 01:15:00|2026-09-14 01:20:00|Barranquilla|7.394232E8    |100        |
|2026-09-14 01:15:00|2026-09-14 01:20:00|Bogotá      |3.3101704E9   |407        |
|2026-09-14 01:15:00|2026-09-14 01:20:00|Bucaramanga |5.755556E8    |75         |
|2026-09-14 01:15:00|2026-09-14 01:20:00|Cali        |1.1887859E9   |143        |
|2026-09-14 01:15:00|2026-09-14 01:20:00|Medellín    |1.4322899E9   |197        |
|2026-09-14 01:15:00|2026-09-14 01:20:00|Otro        |5.682191E8    |78         |
```

Las 12 filas de las ventanas 00:45 y 00:50 no cambiaron. Con eventos de las ~01:17, el
watermark pasó a ~01:07, así que esas dos ventanas quedaron **cerradas** a partir de aquí.

Nota: una primera lectura hecha mientras el batch 10 todavía escribía devolvió números
inconsistentes (12 filas con 4.000 pedidos), porque cada acción de Spark resolvió una versión
distinta de la tabla. La lectura de arriba se hizo con `commits/10` ya escrito.

## Prueba 2: reinicio sin checkpoint

**Qué se hizo** (~01:19 UTC):

```bash
docker stop lab2b-stream
docker run --rm -v lab2b-lake:/tmp/lake st1630-lab2a-spark rm -rf /tmp/lake/checkpoints/lab2b-kafka
docker start lab2b-stream
```

Solo se borró el checkpoint; la tabla Delta quedó intacta con 18 filas y 4.000 pedidos.

**Predicción del equipo antes de correrla:** 18 filas, 4.000 pedidos y Bogotá 00:50 en 806.

**Qué pasó.** Sin checkpoint, Spark volvió a `startingOffsets="earliest"`, sin estado y sin
watermark:

```
01:20:27  [batch 0] 18 filas de ventana actualizadas
01:20:41  [batch 1] 0 filas de ventana actualizadas
```

El checkpoint nuevo empieza en `offsets/0` = `{"0":2228, "1":1083, "2":283, "3":406}`: releyó
**los 4.000 pedidos del topic en un solo batch** y recalculó las 18 ventanas+región desde cero.

Tabla después (versión 7): `filas_totales = 18`, `suma_num_pedidos = 4000`,
`duplicados_ventana_region = 0`, Bogotá 00:50–00:55 en **806** y Bogotá 00:45–00:50 en 429.
Las 18 filas quedaron idénticas a la versión 6. La predicción se cumplió.

## Métricas de cada MERGE

`DESCRIBE HISTORY`, columna `operationMetrics`:

| Versión | Qué fue | `numSourceRows` | `numTargetRowsUpdated` | `numTargetRowsInserted` |
|---|---|---|---|---|
| 1 | corrida 1, segunda mitad (crece la ventana 00:45) | 6 | 6 | 0 |
| 2 | corrida 2, primera mitad (ventana nueva 00:50) | 6 | 0 | 6 |
| 3 | corrida 2, segunda mitad | 6 | 6 | 0 |
| 4 | corrida 3, primera mitad (crece la ventana 00:50) | 6 | 6 | 0 |
| 5 | corrida 3, segunda mitad | 6 | 6 | 0 |
| 6 | corrida 4 (ventana nueva 01:15) | 6 | 0 | 6 |
| **7** | **reproceso completo sin checkpoint** | **18** | **18** | **0** |

La versión 0 es el `WRITE` inicial del batch 1.

## Qué demuestran las pruebas de reinicio

- **Con checkpoint, Spark no reprocesa lo confirmado**, ni siquiera tras una caída abrupta:
  retoma desde el último `commits/N` y el siguiente batch lee solo offsets nuevos.
- **Sin checkpoint, reprocesa todo el topic, y el `MERGE` lo vuelve inofensivo:** la
  versión 7 actualizó 18 filas e insertó 0. Con `append`, la tabla habría pasado a 36 filas y
  8.000 pedidos.
- **El reproceso fue exacto porque entró en un solo batch.** Si se hubiera leído en varios
  batches (por ejemplo, con `maxOffsetsPerTrigger`), el watermark habría avanzado con los
  pedidos de las 01:17 y podría haber descartado los de las ventanas 00:45 y 00:50 al
  reprocesarlas. El `MERGE` evita duplicados, pero no recupera datos descartados por el
  watermark.
