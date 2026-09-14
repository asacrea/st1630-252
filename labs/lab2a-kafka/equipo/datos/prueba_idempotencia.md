# Prueba de idempotencia — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 2026-08-31
**Estudiantes:**
Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co · Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
Sebastián Durán Fernández — sduranf@eafit.edu.co · Daniel Arcila Salazar — darcilas1@eafit.edu.co

> Prueba ejecutada realmente sobre el pipeline de esta entrega. Todos los
> números y logs de abajo son salidas de terminal de nuestra propia corrida, no
> valores de ejemplo. El entorno de ejecución está descrito en
> `../README.md` (el consumidor corre en un contenedor Linux con
> PySpark 3.5.6 + Delta 3.3.0 porque PySpark no puede escribir en disco en
> Windows sin `winutils.exe`).

## Resumen en una línea

El consumidor procesó **6 mensajes** al reiniciar, pero Bronze creció solo
en **5 filas**: el mensaje reentregado (`partition=1 offset=15`) ya estaba
ingestado y el `MERGE ... ON pedido_id` lo actualizó en vez de insertarlo.
El log de transacciones de Delta lo confirma de forma independiente:
**versión 16 → `numTargetRowsInserted = 0`, `numTargetRowsMatchedUpdated = 1`**,
la única versión de todo el historial con un match.

## Los 5 pasos

### Paso 1 — Ejecutar el consumidor hasta procesar ~10 mensajes

Se dejó correr hasta 15 confirmaciones `[OK]` (offsets 0–14 de la
partición 1):

```
[OK] offset=7  partition=1 pedido_id=0f245e07-8dca-4df1-90b2-dca81b7bf9a9
[OK] offset=8  partition=1 pedido_id=6a56b42a-caf3-413c-8db6-0192254f44ec
[OK] offset=9  partition=1 pedido_id=b6ba7855-0a2c-4e84-b01d-f82a6908c761
[OK] offset=10 partition=1 pedido_id=938204dd-6f57-43ed-86e3-3d788c0046b1
[OK] offset=11 partition=1 pedido_id=731bf269-2409-41ac-83fb-8bb598cec682
[OK] offset=12 partition=1 pedido_id=a46b4a19-d037-48d5-99e2-82b9e805c8ef
[OK] offset=13 partition=1 pedido_id=23cf6392-ab64-46d4-9318-4325788a95cb
[OK] offset=14 partition=1 pedido_id=641c3a0a-a58f-4f54-a29e-81e8de5f1ccb
```

Conteo de líneas `[OK]` en el log completo: **15**.

### Paso 2 — Detenerlo sin commitear

En vez de `Ctrl+C` se usó **`SIGKILL`** (`docker kill --signal=KILL`). La
razón es que `Ctrl+C` envía `SIGINT`, que el script atrapa con el
`except KeyboardInterrupt` y sale de forma ordenada; `SIGKILL` no se puede
atrapar y mata el proceso en el instante exacto, que es justamente el
"el consumidor se cayó" que la garantía at-least-once tiene que aguantar.

```
$ docker kill --signal=KILL lab2a-consumidor
$ docker inspect -f "{{.State.Status}} exitcode={{.State.ExitCode}}" lab2a-consumidor
exited exitcode=137          # 137 = 128 + 9 (SIGKILL)
```

**El proceso murió dentro de la ventana crítica.** La evidencia es que el
log tiene 15 líneas `[OK]` (offsets 0–14) pero Bronze quedó con 16 filas
(offsets 0–**15**): el mensaje `offset=15` alcanzó a completar su MERGE,
pero el proceso murió antes de llegar al `consumer.commit()` y antes de
imprimir su `[OK]`. Ese es exactamente el escenario que la Pregunta 1 de
`../kafka_design.md` describe, y ocurrió sin forzarlo.

Estado del consumer group tras la caída:

```
$ docker exec st1630-lab2a-kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 --describe --group analytics-group

Consumer group 'analytics-group' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
analytics-group pedidos-ventas  0          0               556             556
analytics-group pedidos-ventas  1          15              263             248
analytics-group pedidos-ventas  2          0               85              85
analytics-group pedidos-ventas  3          0               96              96
```

`CURRENT-OFFSET = 15` en la partición 1: Kafka da por leídos los offsets
0–14 y va a **reentregar el 15**, aunque el 15 ya esté escrito en Bronze.

### Paso 3 — Contar los registros en Bronze (N)

```
=== CONTEO BRONZE [ANTES de reiniciar] ===
filas_totales      = 16
pedido_id_distintos= 16
duplicados         = 0

--- rango de offsets ingestados por partición ---
+----------------+----------+----------+
|_kafka_partition|offset_min|offset_max|
+----------------+----------+----------+
|               1|         0|        15|
+----------------+----------+----------+

--- ultimas filas ingestadas ---
+----------------+-------------+------------------------------------+--------------------------------+
|_kafka_partition|_kafka_offset|pedido_id                           |_ingested_at                    |
+----------------+-------------+------------------------------------+--------------------------------+
|1               |15           |a4d57b68-42a1-4f89-a37d-b06e51f56487|2026-08-31T22:38:15.331725+00:00|
|1               |14           |641c3a0a-a58f-4f54-a29e-81e8de5f1ccb|2026-08-31T22:38:09.093614+00:00|
+----------------+-------------+------------------------------------+--------------------------------+
```

**N = 16** filas, 16 `pedido_id` distintos.
Fila testigo: `offset=15` → `pedido_id=a4d57b68-42a1-4f89-a37d-b06e51f56487`,
ingestada a las **22:38:15.331725 UTC**.

### Paso 4 — Reiniciar el consumidor

Primeras líneas del log al arrancar de nuevo:

```
[OK] offset=15 partition=1 pedido_id=a4d57b68-42a1-4f89-a37d-b06e51f56487   <-- REENTREGADO
[OK] offset=16 partition=1 pedido_id=84e6e767-8411-4f22-9d0b-ea820681f315
[OK] offset=17 partition=1 pedido_id=3a477e37-4203-4dbd-a37f-8735d81af51f
```

El primer mensaje que Kafka entrega es el `offset=15`, con el **mismo
`pedido_id`** que ya figuraba en Bronze en el Paso 3. Kafka cumplió su
parte del contrato at-least-once: entregó al menos una vez, y en este caso
entregó dos.

Se dejó correr hasta 6 confirmaciones `[OK]` (offsets 15, 16, 17, 18, 19 y 20)
y se volvió a cortar.

### Paso 5 — Contar los registros en Bronze otra vez (N')

```
=== CONTEO BRONZE [DESPUES de reiniciar] ===
filas_totales      = 21
pedido_id_distintos= 21
duplicados         = 0

--- rango de offsets ingestados por partición ---
+----------------+----------+----------+
|_kafka_partition|offset_min|offset_max|
+----------------+----------+----------+
|               1|         0|        20|
+----------------+----------+----------+
```

**N' = 21** filas, 21 `pedido_id` distintos, **0 duplicados**.

## Interpretación

### ¿`N` es igual a `N'`? → No, y el enunciado literal no aplica aquí

`N = 16` y `N' = 21`. La comparación literal "N debe seguir siendo N" del
enunciado solo se cumple si al reiniciar el consumidor **no lee ningún
mensaje nuevo**. En esta corrida sí leyó: quedaban 985 mensajes pendientes
en el topic, así que apenas reprocesó el `offset=15` siguió con los
offsets 16 a 20, que son pedidos que Bronze nunca había visto.

La pregunta que la prueba realmente responde no es "¿el conteo se quedó
quieto?" sino **"¿el mensaje reentregado produjo una fila extra?"**. Y la
respuesta es no. La aritmética lo separa sin ambigüedad:

| Medición | Valor |
|---|---|
| Mensajes procesados tras el reinicio (líneas `[OK]`) | **6** (offsets 15–20) |
| Crecimiento real de Bronze | **+5** filas (16 → 21) |
| Diferencia | **1** — el `offset=15`, que ya estaba |
| `filas_totales − pedido_id_distintos` | **0** duplicados |

Si el consumidor usara `.mode("append")` en vez de `merge()`, Bronze
habría quedado en 22 filas con un `pedido_id` repetido. Quedó en 21 con
cero repetidos.

### Prueba directa 1 — la fila reentregada es una sola, y fue reescrita

```
FILAS CON partition=1 offset=15 : 1

+------------------------------------+----------------+-------------+--------------------------------+-----------+
|pedido_id                           |_kafka_partition|_kafka_offset|_ingested_at                    |region     |
+------------------------------------+----------------+-------------+--------------------------------+-----------+
|a4d57b68-42a1-4f89-a37d-b06e51f56487|1               |15           |2026-08-31T22:40:41.088812+00:00|Bucaramanga|
+------------------------------------+----------------+-------------+--------------------------------+-----------+
```

Hay **una** fila, no dos. Y su `_ingested_at` pasó de
`22:38:15.331725` (Paso 3) a `22:40:41.088812`: la segunda entrega **sí**
tocó la tabla — el `whenMatchedUpdateAll()` pisó la fila — pero no la
duplicó. Esto descarta la explicación alternativa de que el consumidor
simplemente hubiera ignorado el mensaje reentregado.

### Prueba directa 2 — el log de transacciones de Delta

La evidencia más fuerte, porque no depende de nuestros conteos sino de lo que
Delta registró por su cuenta. Filtrando el historial por las versiones
cuyo MERGE encontró un match:

```
### VERSIONES DELTA DONDE EL MERGE HIZO UPDATE (match por pedido_id) ###
+-------+-----------------------+---------+----------+----------------------+
|version|timestamp              |operation|insertadas|actualizadas_por_match|
+-------+-----------------------+---------+----------+----------------------+
|16     |2026-08-31 22:41:01.214|MERGE    |0         |1                     |
+-------+-----------------------+---------+----------+----------------------+

total de MERGE que actualizaron en vez de insertar: 1
```

**Una sola versión en todo el historial** (la 16) tiene
`numTargetRowsMatchedUpdated = 1` e `numTargetRowsInserted = 0`. Todas las
demás versiones son `insertadas = 1, actualizadas = 0`. Esa versión 16 es
el reproceso del `offset=15`: Delta vio que el `pedido_id` ya existía,
tomó la rama `whenMatchedUpdateAll()` y actualizó la fila existente en
lugar de agregar una nueva.

### Conclusión

El pipeline es at-least-once del lado de Kafka (el mensaje se entregó dos
veces) e idempotente del lado de Bronze (la fila existe una sola vez). Las
dos propiedades juntas son lo que hace que el diseño sea seguro: se puede
tolerar la reentrega precisamente porque el destino la absorbe sin
duplicar. El commit manual después del MERGE es lo que garantiza que la
reentrega ocurra cuando tiene que ocurrir; el `MERGE ... ON pedido_id` es
lo que garantiza que esa reentrega no cueste nada.

## Cómo reproducir esta prueba

```bash
# 1. Levantar el clúster (ver ../README.md sobre el override del CLUSTER_ID)
cd labs/lab2a-kafka
docker compose -f docker-compose.yml -f equipo/docker-compose.override.yml up -d
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas \
  --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092

# 2. Publicar los 1.000 pedidos
python equipo/scripts/productor_kafka.py

# 3. Arrancar el consumidor, dejarlo procesar ~15 mensajes y matarlo en seco
docker run -d --name lab2a-consumidor --network lab2a-kafka_default ... \
  st1630-lab2a-spark python /scripts/consumidor_kafka.py
docker kill --signal=KILL lab2a-consumidor

# 4. Contar Bronze antes y despues de reiniciar
docker run --rm ... st1630-lab2a-spark python /scripts/contar_bronze.py "ANTES"
```

El comando completo con todos los volúmenes y variables de entorno está en
`../README.md`, sección "Cómo correr esta entrega".
