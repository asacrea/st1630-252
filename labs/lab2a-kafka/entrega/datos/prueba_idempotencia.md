# Prueba de idempotencia — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 31/08/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

> Evidencia real de la ejecución del pipeline (Parte 2.4 del `../../README.md`).

## Los 5 pasos (lo que efectivamente hicimos)

| Paso | Qué hicimos |
|---|---|
| 1 | Corrimos el consumidor hasta procesar 16 mensajes |
| 2 | Lo matamos **sin gracia** (`kill -9`, no Ctrl+C) para simular una caída real a mitad de proceso |
| 3 | Contamos Bronze → **N = 17** |
| 4 | Rebobinamos el offset del grupo 5 posiciones y reiniciamos el consumidor |
| 5 | Contamos Bronze otra vez → **N' = 17** |

**Por qué `kill -9` y no Ctrl+C:** Ctrl+C entra por el `except
KeyboardInterrupt` del script, que cierra el consumidor ordenadamente.
`kill -9` no le da al proceso ninguna oportunidad de limpiar nada — es
mucho más parecido a lo que pasa en producción cuando el pod se muere,
el nodo se reinicia o el proceso se queda sin memoria.

**Por qué además rebobinamos el offset (paso 4):** el `kill -9` por sí
solo ya nos dejó un mensaje sin commitear (ver más abajo), pero uno
solo. Rebobinar el offset del grupo con
`kafka-consumer-groups --reset-offsets --shift-by -5` fuerza a Kafka a
reenviar **6 mensajes que ya estaban en Bronze**, que es exactamente lo
que hace at-least-once cuando se pierden varios commits seguidos. Con 6
reenvíos en vez de 1, la prueba es mucho más contundente.

```bash
docker exec st1630-lab2a-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --group analytics-group \
  --topic pedidos-ventas:0 --reset-offsets --shift-by -5 --execute
# GROUP            TOPIC           PARTITION  NEW-OFFSET
# analytics-group  pedidos-ventas  0          11
```

## Evidencia — log del consumidor (antes de detener)

Últimas líneas antes del `kill -9`
(16 líneas `[OK]` en total, offsets 0 a 15):

```
[OK] offset=10 partition=0 pedido_id=daa40a28-1a95-4b60-86e0-b56487b0adeb
[OK] offset=11 partition=0 pedido_id=0b80c227-e164-4588-a281-7bade753d6f2
[OK] offset=12 partition=0 pedido_id=cddd6223-fd99-4194-bf06-f5821f1650c8
[OK] offset=13 partition=0 pedido_id=654b97d9-bfdc-4a64-8e6c-7f69db56a333
[OK] offset=14 partition=0 pedido_id=2e5f5a32-2dc0-42cd-8676-f00ee590743b
[OK] offset=15 partition=0 pedido_id=f6a745de-6f6e-42b8-81fc-8a9ddd3d10e0
```

Estado del consumer group inmediatamente después del `kill -9`:

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
analytics-group pedidos-ventas  0          16              543             527
analytics-group pedidos-ventas  1          0               316             316
analytics-group pedidos-ventas  2          0               58              58
analytics-group pedidos-ventas  3          0               83              83
```

## Evidencia — conteo de Bronze ANTES de reiniciar

```
==========================================================
Bronze: /tmp/lake/bronze/pedidos
  filas totales          N = 17
  pedido_id distintos      = 17
  duplicados por pedido_id = 0
  rango de offsets ingeridos por partición:
+----------------+----------+----------+-----+
|_kafka_partition|min_offset|max_offset|filas|
+----------------+----------+----------+-----+
|               0|         0|        16|   17|
+----------------+----------+----------+-----+
==========================================================
```

### El hallazgo más importante de toda la prueba

Los números no cuadran a primera vista, y ese descuadre **es** el
resultado:

| Fuente | Valor |
|---|---|
| Líneas `[OK]` en el log de run1 | 16 (offsets 0 … 15) |
| `CURRENT-OFFSET` commiteado del grupo | 16 (⇒ commiteados 0 … 15) |
| Filas en Bronze | 17 (offsets 0 … **16**) |

El mensaje del **offset 16 está en Bronze pero nunca fue commiteado**, y
tampoco alcanzó a imprimir su `[OK]`. Es decir: el `kill -9` cayó justo
en la ventana entre `merge_a_bronze(fila)` y `consumer.commit()`.

Ese es **literalmente** el escenario de falla que motiva todo el lab, y
nos ocurrió de verdad, sin forzarlo. Kafka, al no ver el commit del
offset 16, está obligado a reenviarlo — y por eso el conteo de Bronze
no puede cambiar cuando lo haga.

## Evidencia — log del consumidor (al reiniciar)

`Completo (lo detuvimos apenas reprocesó el
offset 16, para que ningún mensaje nuevo contaminara la comparación):

```
Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: kafka:29092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
[OK] offset=11 partition=0 pedido_id=0b80c227-e164-4588-a281-7bade753d6f2
[OK] offset=12 partition=0 pedido_id=cddd6223-fd99-4194-bf06-f5821f1650c8
[OK] offset=13 partition=0 pedido_id=654b97d9-bfdc-4a64-8e6c-7f69db56a333
[OK] offset=14 partition=0 pedido_id=2e5f5a32-2dc0-42cd-8676-f00ee590743b
[OK] offset=15 partition=0 pedido_id=f6a745de-6f6e-42b8-81fc-8a9ddd3d10e0
[OK] offset=16 partition=0 pedido_id=98bc1eac-4ca7-4004-bfb0-7b88d936f09b
```

Los `pedido_id` de los offsets 11 a 15 son **idénticos** a los del log
de run1 (compárense línea por línea con el bloque de arriba): no son
pedidos nuevos, es el mismo mensaje entregado dos veces. El offset 16
aparece por primera vez en un log, aunque su fila ya llevaba rato en
Bronze.

## Evidencia — conteo de Bronze DESPUÉS de reiniciar

```
==========================================================
Bronze: /tmp/lake/bronze/pedidos
  filas totales          N = 17
  pedido_id distintos      = 17
  duplicados por pedido_id = 0
  rango de offsets ingeridos por partición:
+----------------+----------+----------+-----+
|_kafka_partition|min_offset|max_offset|filas|
+----------------+----------+----------+-----+
|               0|         0|        16|   17|
+----------------+----------+----------+-----+
  filas con _kafka_offset <= 16: 17 (pedido_id distintos: 17)
==========================================================
```

### Prueba adicional: el MERGE sí tocó las filas (no las ignoró)

Un `N = N'` por sí solo no distingue entre "el MERGE actualizó la fila
existente" y "el consumidor nunca leyó nada". La columna de
trazabilidad `_ingested_at` lo desambigua — los offsets 9 y 10 (no
reprocesados) conservan su timestamp de run1, mientras que los offsets
11 a 16 (reprocesados) tienen el timestamp de run2:

```
+-------------+------------------------------------+--------------------------------+
|_kafka_offset|pedido_id                           |_ingested_at                    |
+-------------+------------------------------------+--------------------------------+
|9            |ba5a4326-cf59-4f7f-b2a8-e2e671a78123|2026-08-31T18:19:58.898756+00:00|  <- run1
|10           |daa40a28-1a95-4b60-86e0-b56487b0adeb|2026-08-31T18:20:01.304598+00:00|  <- run1
|11           |0b80c227-e164-4588-a281-7bade753d6f2|2026-08-31T18:22:07.636575+00:00|  <- run2 (reprocesado)
|12           |cddd6223-fd99-4194-bf06-f5821f1650c8|2026-08-31T18:22:26.657992+00:00|  <- run2 (reprocesado)
|13           |654b97d9-bfdc-4a64-8e6c-7f69db56a333|2026-08-31T18:22:30.174718+00:00|  <- run2 (reprocesado)
|14           |2e5f5a32-2dc0-42cd-8676-f00ee590743b|2026-08-31T18:22:35.891156+00:00|  <- run2 (reprocesado)
|15           |f6a745de-6f6e-42b8-81fc-8a9ddd3d10e0|2026-08-31T18:22:39.738996+00:00|  <- run2 (reprocesado)
|16           |98bc1eac-4ca7-4004-bfb0-7b88d936f09b|2026-08-31T18:22:44.390654+00:00|  <- run2 (reprocesado)
+-------------+------------------------------------+--------------------------------+
```

Ese salto de ~2 minutos en `_ingested_at` es la huella del
`whenMatchedUpdateAll()`: el MERGE **encontró** el `pedido_id`, entró
por la rama de UPDATE y sobrescribió la fila (incluido el timestamp),
en vez de entrar por `whenNotMatchedInsertAll()` y crear una segunda.

## Interpretación

¿`N` es igual a `N'`? → **Sí. N = N' = 17.**

Seis mensajes (offsets 11 a 16 de la partición 0) fueron entregados y
procesados **dos veces** — el log de run2 lo muestra explícitamente y
`_ingested_at` confirma que el MERGE los volvió a escribir — y Bronze
siguió teniendo exactamente 17 filas con 17 `pedido_id` distintos: cero
duplicados.

La conclusión concreta sobre nuestro pipeline:

1. **Kafka duplicó la entrega**, como debe hacerlo en at-least-once:
   como `enable_auto_commit=False` y el `consumer.commit()` va después
   del MERGE, todo mensaje sin commit se vuelve a entregar. Ningún
   mensaje se perdió, ni siquiera el del offset 16, que murió en la
   ventana exacta entre el MERGE y el commit.
2. **Bronze no duplicó el dato**, porque el `MERGE ... ON
   existente.pedido_id = nuevo.pedido_id` es idempotente: la segunda
   vez que llega el mismo `pedido_id` hace UPDATE, no INSERT.
3. La combinación de (1) y (2) es lo que hace que el pipeline sea
   **at-least-once en la entrega y exactly-once en el efecto**: puede
   procesar un mensaje N veces, pero el estado final de Bronze es el
   mismo que si lo hubiera procesado una sola vez.

Si en cambio hubiéramos dejado `enable_auto_commit=True`, el commit del
offset 16 habría podido ocurrir **antes** del MERGE: al reiniciar,
Kafka habría retomado en el 17 y ese pedido no estaría en ninguna parte
— pérdida silenciosa, sin ningún error en el log que la delatara.

Y si el MERGE fuera un `.mode("append")`, las 6 reentregas habrían
dejado `N' = 23` con 17 `pedido_id` distintos: 6 filas duplicadas.

## Segunda prueba, sin buscarla: 512 reprocesos a escala completa

La prueba de los 5 pasos usa 6 mensajes reentregados. Al drenar los
1.000 mensajes del topic apareció una falla mucho más grande que sirve
como segunda prueba, esta vez sin que la provocáramos.

**Qué pasó.** El `KafkaConsumer` trae `max_poll_records=500` por
defecto: trae un lote de hasta 500 mensajes en un `poll()` y nuestro
loop los procesa uno por uno, con un MERGE de ~2 s cada uno. Eso son
~15 minutos sin volver a llamar a `poll()`, muy por encima de
`max_poll_interval_ms` (5 min). El broker dio por muerto al consumidor,
lo expulsó del grupo, y a partir de ese momento **todos** los
`consumer.commit()` fallaron:

```
[ERROR] offset=246 partition=0 no se commiteó -- se reprocesará. Causa: CommitFailedError: Offset commit cannot be completed since t...
[ERROR] offset=247 partition=0 no se commiteó -- se reprocesará. Causa: CommitFailedError: ...
```

Lo importante: **el MERGE seguía funcionando**. Bronze siguió creciendo
mientras el offset commiteado del grupo se quedó clavado en 246. En un
momento Bronze tenía 539 filas y el grupo seguía en `CURRENT-OFFSET=246`
— 293 mensajes escritos sin commit. Al reiniciar, Kafka reentregó los
293.

**Contabilidad de todas las corridas** (`log_consumidor_run*.txt`):

| Corrida | `[OK]` (MERGE + commit) | `[ERROR]` (MERGE sin commit) |
|---|---:|---:|
| run1 (interrumpida con `kill -9`) | 16 | 0 |
| run2 (reproceso tras rebobinar) | 6 | 0 |
| run3 (drenaje) | 98 | 2 |
| run4 (drenaje) | 131 | 292 |
| run5 (drenaje) | 180 | 213 |
| run6 (drenaje, ya con `max_poll_records=10`) | 574 | 0 |
| **Total de MERGE ejecutados** | **1.005** | **507** |

```
MERGEs totales ejecutados : 1.512
Mensajes únicos publicados: 1.000
Mensajes procesados más de una vez: 512
```

**Estado final de Bronze:**

```
==========================================================
Bronze: /tmp/lake/bronze/pedidos
  filas totales          N = 1000
  pedido_id distintos      = 1000
  duplicados por pedido_id = 0
  rango de offsets ingeridos por partición:
+----------------+----------+----------+-----+
|_kafka_partition|min_offset|max_offset|filas|
+----------------+----------+----------+-----+
|               0|         0|       542|  543|
|               1|         0|       315|  316|
|               2|         0|        57|   58|
|               3|         0|        82|   83|
+----------------+----------+----------+-----+
==========================================================
```

Y el consumer group, con `lag = 0` en las 4 particiones
(`consumer_group_lag_cero.txt`):

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
analytics-group pedidos-ventas  0          543             543             0
analytics-group pedidos-ventas  1          316             316             0
analytics-group pedidos-ventas  2          58              58              0
analytics-group pedidos-ventas  3          83              83              0
```

**Conclusión:** se ejecutaron 1.512 MERGE para 1.000 mensajes — más de
un 50 % de reprocesamiento — y Bronze terminó con exactamente 1.000
filas y 1.000 `pedido_id` distintos, con los offsets completos de las 4
particiones (543 + 316 + 58 + 83 = 1.000, que es exactamente lo que
publicó el productor). Ni un mensaje perdido, ni una fila duplicada.

Esa es la prueba más fuerte que tenemos de que la combinación
*at-least-once + MERGE idempotente* funciona: el pipeline se rompió de
verdad, tres veces, y el resultado en Bronze es indistinguible del de
una corrida limpia.

## Cómo reproducir estos números

```bash
# 1. Levantar la infraestructura y crear el topic (Parte 0)
docker compose up -d
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas \
  --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092

# 2. Publicar los 1.000 pedidos
python productor_kafka.py

# 3. Consumir, matar el proceso a los ~50 s, contar, rebobinar, reconsumir, contar
#    (ver ../README.md para el detalle del entorno con el que corrimos Spark)
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 consumidor_kafka.py
spark-submit --packages io.delta:delta-spark_2.12:3.1.0 contar_bronze.py
```
