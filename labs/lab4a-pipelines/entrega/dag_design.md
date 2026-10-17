# dag_design.md — Lab 4a: Diseño del DAG

**Curso:** ST1630 · **Semana:** S11 · **Fecha:** 16/10/2026
**Estudiantes:**  
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

Toda la evidencia de este documento sale de nuestra ejecución local
(Airflow 2.9.3 en Docker, dataset generado con `gen_dataset.py`, semilla
1630). Las horas de los logs están en UTC.

## Justificación del schedule

**schedule_interval elegido:** `@daily`

**Justificación:** El dato llega por día: cada corrida procesa una
partición `fecha={ds}` del CSV. Las tablas de Gold (`ventas_por_region`
por mes y `top_productos`) son de reporte: el equipo de Nequi las
consultaría una vez al día, al empezar la jornada, para ver cómo cerró
el día anterior. Correr más seguido no aporta nada, porque el día no
cambia hasta que se cierra. Correr menos seguido (`@weekly`) retrasaría
hasta 7 días el reporte de un día malo.

Un detalle que encontramos: el DAG usa `start_date=datetime(2026, 1, 1)`
sin zona horaria, así que Airflow lo interpreta en UTC. La corrida de
`ds=2026-01-03` arranca cuando cierra su intervalo, el 2026-01-04 a las
00:00 UTC, que en Colombia son las **19:00 del día 3**. Es decir, la
corrida del día 3 se dispararía antes de que ese día termine en
Colombia. En producción pondríamos el `start_date` con zona horaria
(`pendulum.datetime(2026, 1, 1, tz="America/Bogota")`) para que corra a
la medianoche de Colombia.

**¿Por qué `catchup=False`?** Con `catchup=True`, al activar un DAG que
estuvo 30 días apagado, el scheduler crea de inmediato una corrida por
cada intervalo pendiente: 30 corridas en cola. En nuestro caso sería
peor, porque el `start_date` es 2026-01-01 y lo activamos el 2026-10-16:
serían unas 288 corridas. Con `max_active_runs=1` y unos 2 minutos por
corrida, eso es más de 9 horas de Spark, y casi todas para fechas sin
datos (el dataset solo cubre del 01-01 al 01-10).

Con `catchup=False` pasó lo esperado: al activar el DAG, el scheduler
creó una sola corrida, la del último intervalo
(`scheduled__2026-10-16T00:00:00+00:00`, 22 s, `success`, sin filas que
ingerir), y las fechas históricas las cargamos a propósito con
`airflow dags backfill` sobre el rango que nos interesaba. `catchup`
decide cuándo se procesa el pasado; los datos procesados son los mismos
en ambos casos, gracias a la idempotencia.

## Idempotencia — ¿qué significa y cómo la logré?

**Definición:** Un pipeline es idempotente si ejecutarlo N veces con la
misma entrada produce el mismo resultado que ejecutarlo 1 vez.

**Cómo la implementé en `ingest_bronze` (TODO 1):**

```python
df_dia.write.mode("overwrite").parquet(destino)   # destino = f"{BRONZE_DIR}/fecha={ds}"
```

La ruta de escritura es la carpeta del día, no la tabla entera. Por eso
`overwrite` reemplaza solo `fecha={ds}`: Spark borra lo que había en
esa carpeta (incluidos restos de un intento que falló a medias) y
escribe la partición nueva. Escribir dos veces el mismo día deja la
partición igual a como la deja una sola escritura, y los demás días no
se tocan. Con `append`, cada reintento agregaría otro archivo parquet a
la carpeta y la partición tendría las filas repetidas.

**Cómo la implementé en `transform_silver` (TODO 3, el MERGE):**

```python
silver_table.alias("target").merge(
    df_transformado.alias("source"), "target.pedido_id = source.pedido_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

La condición usa `pedido_id`, la clave de negocio. Si el pedido ya
existe en Silver, se actualiza en su lugar. Si no existe, se inserta.
Al re-ejecutar el mismo `ds`, todos los pedidos de ese día ya existen,
así que el MERGE solo hace *updates* con los mismos valores y Silver no
crece. Con un `append`, cada re-ejecución insertaría otra vez las filas
del día. En nuestra prueba, el segundo backfill habría dejado Silver en
5.640 filas en vez de 2.820.

Sobre la pregunta del código (el MERGE se ejecuta, el contenedor se cae
antes de que Airflow registre el éxito y el task se reintenta): el
commit de Delta es atómico. Si la caída ocurre antes del commit, la
tabla queda como estaba y el reintento hace el MERGE completo. Si ocurre
después, el reintento encuentra todos los `pedido_id` ya presentes y
solo actualiza. En ningún caso se duplica nada. Eso fue exactamente lo
que pasó en nuestro segundo backfill.

También agregamos `max_active_runs=1` al DAG, fuera de los TODO. El
backfill corre varias fechas en paralelo, y con Silver todavía vacío dos
`transform_silver` pueden entrar a la vez en la rama "Silver no existe →
`overwrite`", de modo que una fecha borra a la otra. Con Silver ya
creado, dos MERGE simultáneos sobre la misma tabla chocan con
`ConcurrentAppendException`. La idempotencia de cada task por separado
no protege contra esa concurrencia entre tasks.

**Prueba de idempotencia que realicé:** Corrimos dos veces el backfill
del mismo rango, del 2026-01-01 al 2026-01-07: la segunda vez con
`--reset-dagruns`, que reinicia las corridas existentes y las vuelve a
ejecutar completas. Después de cada pasada contamos Bronze (por
partición y por número de archivos), Silver (filas y `pedido_id`
distintos) y Gold, y leímos el historial de la tabla Delta de Silver.
Los valores esperados, calculados aparte desde el CSV, eran 2.957 filas
en Bronze, 2.820 en Silver (sin devueltos) y una suma de
2.402.312.700.

```
===== CONTEOS tras backfill #1 =====              ===== CONTEOS tras backfill #2 (mismo rango) =====
fecha=2026-01-01: 325 filas, 1 archivo(s) parquet  fecha=2026-01-01: 325 filas, 1 archivo(s) parquet
fecha=2026-01-02: 399 filas, 1 archivo(s) parquet  fecha=2026-01-02: 399 filas, 1 archivo(s) parquet
fecha=2026-01-03: 529 filas, 1 archivo(s) parquet  fecha=2026-01-03: 529 filas, 1 archivo(s) parquet
fecha=2026-01-04: 398 filas, 1 archivo(s) parquet  fecha=2026-01-04: 398 filas, 1 archivo(s) parquet
fecha=2026-01-05: 311 filas, 1 archivo(s) parquet  fecha=2026-01-05: 311 filas, 1 archivo(s) parquet
fecha=2026-01-06: 474 filas, 1 archivo(s) parquet  fecha=2026-01-06: 474 filas, 1 archivo(s) parquet
fecha=2026-01-07: 521 filas, 1 archivo(s) parquet  fecha=2026-01-07: 521 filas, 1 archivo(s) parquet
Bronze total: 2957 filas                           Bronze total: 2957 filas
Silver: 2820 filas, 2820 pedido_id distintos,      Silver: 2820 filas, 2820 pedido_id distintos,
        duplicados=0                                       duplicados=0
ventas_por_region: 6 filas,                        ventas_por_region: 6 filas,
        suma total_ventas=2,402,312,700.0                  suma total_ventas=2,402,312,700.0
top_productos: 10 filas,                           top_productos: 10 filas,
        suma total_ventas=2,402,312,700.0                  suma total_ventas=2,402,312,700.0
```

Historial Delta de Silver después de las dos pasadas. La primera
inserta; la segunda solo actualiza (`numTargetRowsInserted=0`):

```
v0 WRITE: numOutputRows=316
v1 MERGE: numSourceRows=380, numTargetRowsInserted=380, numTargetRowsUpdated=0
v2 MERGE: numSourceRows=498, numTargetRowsInserted=498, numTargetRowsUpdated=0
v3 MERGE: numSourceRows=377, numTargetRowsInserted=377, numTargetRowsUpdated=0
v4 MERGE: numSourceRows=296, numTargetRowsInserted=296, numTargetRowsUpdated=0
v5 MERGE: numSourceRows=450, numTargetRowsInserted=450, numTargetRowsUpdated=0
v6 MERGE: numSourceRows=503, numTargetRowsInserted=503, numTargetRowsUpdated=0
v7 MERGE: numSourceRows=316, numTargetRowsInserted=0, numTargetRowsUpdated=316   <- backfill #2
v8 MERGE: numSourceRows=380, numTargetRowsInserted=0, numTargetRowsUpdated=380
v9 MERGE: numSourceRows=498, numTargetRowsInserted=0, numTargetRowsUpdated=498
v10 MERGE: numSourceRows=377, numTargetRowsInserted=0, numTargetRowsUpdated=377
v11 MERGE: numSourceRows=296, numTargetRowsInserted=0, numTargetRowsUpdated=296
v12 MERGE: numSourceRows=450, numTargetRowsInserted=0, numTargetRowsUpdated=450
v13 MERGE: numSourceRows=503, numTargetRowsInserted=0, numTargetRowsUpdated=503
```

El mismo efecto se ve en los logs de Airflow de `transform_silver` para
`2026-01-03`. En la primera pasada Silver pasa a 1.194 filas
(316 + 380 + 498). En la segunda, con las 7 fechas ya cargadas, queda en
2.820, el mismo total que tenía antes de ese MERGE:

```
attempt=1  [2026-10-17T01:53:02] INFO - transform_silver 2026-01-03: 529 filas Bronze -> 498 filas tras excluir devueltos
attempt=1  [2026-10-17T01:53:21] INFO - Silver tiene ahora 1,194 filas en total.
attempt=2  [2026-10-17T02:08:12] INFO - transform_silver 2026-01-03: 529 filas Bronze -> 498 filas tras excluir devueltos
attempt=2  [2026-10-17T02:08:32] INFO - Silver tiene ahora 2,820 filas en total.
```

## Data quality checks implementados

| Check | Task donde vive | ¿Qué detecta? | ¿Qué hace si falla? |
|---|---|---|---|
| Nulos críticos | `validate_bronze` | Que `pedido_id`, `producto` o `total` tengan más de un 5% de filas nulas en la partición del día. Evalúa las tres columnas antes de fallar, para que el mensaje muestre todas las que superan el umbral con su porcentaje | Lanza `ValueError`. El task queda fallido (con 2 reintentos), `transform_silver` y `build_gold` quedan `upstream_failed`, y Silver y Gold no se tocan |
| Rango de monto | `validate_bronze` | Filas con `total <= 0`, con tolerancia cero: una sola fila basta. Los `total` nulos no cuentan aquí, porque `NULL <= 0` no es verdadero; de esos se encarga el check de nulos | Igual que el anterior: `ValueError` y el pipeline se detiene en ese día |
| Cardinalidad de canal | `validate_bronze` | Que `canal` tenga más de 4 valores distintos | Solo `logger.warning`; el pipeline sigue. Un canal nuevo puede ser legítimo (por ejemplo, Nequi empieza a vender por WhatsApp), y en ese caso todas las ventas son reales y los montos son correctos. Tumbar el pipeline dejaría el reporte del día sin publicar por un cambio que no daña los datos. Un nulo en `total` o un monto negativo sí dan un Gold incorrecto, por eso esos dos checks sí detienen el pipeline. En nuestro dataset hay exactamente 4 canales, así que el warning nunca se disparó |

**¿Tiene sentido el 5% para este dataset?** Para `total` y `producto`
es razonable como tolerancia a ruido. Aun así, cualquier nulo en
`total` que pase el check entra a Silver, y `F.sum` lo ignora, de modo
que Gold subreporta en silencio. Para `pedido_id` el umbral debería ser
**0%**: como explicamos arriba, una fila con `pedido_id` nulo nunca hace
*match* en el MERGE y se vuelve a insertar en cada reintento o backfill.
El umbral actual deja pasar justo los datos que rompen la idempotencia.

**¿Qué check añadirías en un pipeline de producción real?**
1. **Volumen contra el histórico:** que el número de filas del día esté
   dentro de un rango razonable respecto a los últimos 7 días (en
   nuestro dataset, entre 311 y 529 filas). Hoy `ingest_bronze` trata 0
   filas como "día sin ventas" y sigue sin avisar. Para Nequi, un día
   hábil con 0 ventas o con el 10% del volumen normal casi seguro es un
   feed cortado, no un día real.
2. **Unicidad de `pedido_id` dentro de la partición:** un duplicado hace
   fallar el MERGE con un error poco claro. Mejor detectarlo en
   `validate_bronze` con un mensaje explícito.
3. **Consistencia del monto:** que `total ≈ cantidad × precio_unit`
   (con tolerancia por redondeo). Detecta corrupción que el check de
   rango no ve: un total positivo pero equivocado.

## Backfill — ¿qué es y cuándo se usa?

**Definición:** Ejecutar el DAG para intervalos de fechas del pasado,
cada uno como una corrida independiente con su propio `ds`, como si el
scheduler lo hubiera corrido en su momento. Se usa para cargar el
histórico cuando el pipeline es nuevo (nuestro caso), para reprocesar
fechas después de corregir un bug o un dato fuente, y para recuperar
días en los que el pipeline estuvo caído. Solo es seguro si los tasks
son idempotentes: reprocesar un día ya cargado no debe duplicarlo.

**Comando que ejecuté:**

```bash
# Carga inicial (Parte 3)
docker compose exec airflow-scheduler \
  airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline

# Re-ejecución del mismo rango para la prueba de idempotencia
docker compose exec airflow-scheduler \
  airflow dags backfill --reset-dagruns -y -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline

# Fallo provocado (Parte 4)
docker compose exec airflow-scheduler \
  airflow dags backfill -s 2026-01-08 -e 2026-01-08 ventas_colombia_pipeline
```

El primer backfill falló en `ingest_bronze` del 01-01 por un problema de
permisos del volumen. Tras corregirlo, lo relanzamos
con `--reset-dagruns`. El log de ese task muestra 3 intentos fallidos y
uno exitoso.

**¿Por qué es importante que `ingest_bronze` use `{{ ds }}` en vez de,
por ejemplo, "la fecha de hoy" (`datetime.now()`)?** `ds` es la fecha
lógica de la corrida: sale del intervalo de datos y es fija para ese
run, sin importar cuándo se ejecute. Si `ingest_bronze` usara
`datetime.now()`, nuestro backfill del 01-01 al 01-07 (ejecutado el
2026-10-17 en UTC) habría filtrado las 7 veces el CSV por `2026-10-17`:
0 filas en cada corrida y ningún dato histórico cargado, con las 7
corridas en verde. Además, la misma corrida daría resultados distintos
según el día en que se reintente, lo que rompe la idempotencia: un
reintento hecho después de medianoche procesaría otro día distinto del
que falló.

## Fallo provocado — log de `validate_bronze`

Vaciamos la columna `total` en 1 de cada 10 filas del 2026-01-08: 51 de
510 filas (10,00%), por encima del umbral del 5%. Luego corrimos el
backfill de esa fecha.

Intento 3 de 3 (el último), log completo de la task:

```
[2026-10-17T02:41:03.064+0000] {local_task_job_runner.py:120} INFO - ::group::Pre task execution logs
[2026-10-17T02:41:03.107+0000] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-17T02:41:03.117+0000] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-17T02:41:03.118+0000] {taskinstance.py:2306} INFO - Starting attempt 3 of 3
[2026-10-17T02:41:03.132+0000] {taskinstance.py:2330} INFO - Executing <Task(PythonOperator): validate_bronze> on 2026-01-08 00:00:00+00:00
[2026-10-17T02:41:03.140+0000] {standard_task_runner.py:64} INFO - Started process 23498 to run task
[2026-10-17T02:41:03.145+0000] {standard_task_runner.py:90} INFO - Running: ['***', 'tasks', 'run', 'ventas_colombia_pipeline', 'validate_bronze', 'backfill__2026-01-08T00:00:00+00:00', '--job-id', '72', '--pool', 'default_pool', '--raw', '--subdir', 'DAGS_FOLDER/ventas_pipeline.py', '--cfg-path', '/tmp/tmp5gso3ncc']
[2026-10-17T02:41:03.149+0000] {standard_task_runner.py:91} INFO - Job 72: Subtask validate_bronze
[2026-10-17T02:41:03.221+0000] {task_command.py:426} INFO - Running <TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [running]> on host 228f480bb845
[2026-10-17T02:41:03.348+0000] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='st1630' AIRFLOW_CTX_DAG_ID='ventas_colombia_pipeline' AIRFLOW_CTX_TASK_ID='validate_bronze' AIRFLOW_CTX_EXECUTION_DATE='2026-01-08T00:00:00+00:00' AIRFLOW_CTX_TRY_NUMBER='3' AIRFLOW_CTX_DAG_RUN_ID='backfill__2026-01-08T00:00:00+00:00'
[2026-10-17T02:41:03.349+0000] {taskinstance.py:430} INFO - ::endgroup::
[2026-10-17T02:41:11.696+0000] {ventas_pipeline.py:175} INFO - Nulos en 'pedido_id': 0 de 510 filas (0.00%)
[2026-10-17T02:41:11.923+0000] {ventas_pipeline.py:175} INFO - Nulos en 'producto': 0 de 510 filas (0.00%)
[2026-10-17T02:41:12.397+0000] {ventas_pipeline.py:175} INFO - Nulos en 'total': 51 de 510 filas (10.00%)
[2026-10-17T02:41:12.705+0000] {taskinstance.py:441} INFO - ::group::Post task execution logs
[2026-10-17T02:41:12.707+0000] {taskinstance.py:2905} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 465, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 432, in _execute_callable
    return execute_callable(context=context, **execute_callable_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/baseoperator.py", line 401, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 235, in execute
    return_value = self.execute_callable()
                   ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 252, in execute_callable
    return self.python_callable(*self.op_args, **self.op_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/airflow/dags/ventas_pipeline.py", line 235, in validate_bronze
    check_nulos_criticos(df)
  File "/opt/airflow/dags/ventas_pipeline.py", line 180, in check_nulos_criticos
    raise ValueError(
ValueError: Check de nulos críticos FALLÓ (umbral 5%): 'total' tiene 10.00% de nulos (51 de 510 filas)
[2026-10-17T02:41:12.723+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=ventas_colombia_pipeline, task_id=validate_bronze, run_id=backfill__2026-01-08T00:00:00+00:00, execution_date=20260108T000000, start_date=20261017T024103, end_date=20261017T024112
[2026-10-17T02:41:12.741+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 72 for task validate_bronze (Check de nulos críticos FALLÓ (umbral 5%): 'total' tiene 10.00% de nulos (51 de 510 filas); 23498)
[2026-10-17T02:41:12.762+0000] {local_task_job_runner.py:243} INFO - Task exited with return code 1
[2026-10-17T02:41:12.791+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-10-17T02:41:12.797+0000] {local_task_job_runner.py:222} INFO - ::endgroup::
```

Los intentos 1 y 2 fallaron con el mismo mensaje y quedaron como `UP_FOR_RETRY`,
con 5 minutos de espera entre cada uno (`retry_delay`):

```
[2026-10-17T02:30:34] ValueError: Check de nulos críticos FALLÓ (umbral 5%): 'total' tiene 10.00% de nulos (51 de 510 filas)
[2026-10-17T02:30:34] INFO - Marking task as UP_FOR_RETRY ... task_id=validate_bronze, run_id=backfill__2026-01-08T00:00:00+00:00
[2026-10-17T02:35:51] ValueError: Check de nulos críticos FALLÓ (umbral 5%): 'total' tiene 10.00% de nulos (51 de 510 filas)
[2026-10-17T02:35:51] INFO - Marking task as UP_FOR_RETRY ... task_id=validate_bronze, run_id=backfill__2026-01-08T00:00:00+00:00
```

Lo que reportó el backfill y los conteos de después. `transform_silver` y `build_gold` no
llegaron a ejecutarse, y Silver y Gold quedaron exactamente como antes del fallo (la
última versión de Silver sigue siendo la v13; no hubo un MERGE nuevo). Solo Bronze tiene
la partición del 01-08, que es donde se detecta el problema:

```
ERROR - Task instance <TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [failed]> failed
ERROR - Task instance <TaskInstance: ventas_colombia_pipeline.transform_silver backfill__2026-01-08T00:00:00+00:00 [upstream_failed]> with state upstream_failed
ERROR - Task instance <TaskInstance: ventas_colombia_pipeline.build_gold backfill__2026-01-08T00:00:00+00:00 [upstream_failed]> with state upstream_failed
DagRun Finished: ... run_id=backfill__2026-01-08T00:00:00+00:00, run_duration=684.860228, state=failed

===== CONTEOS tras fallo provocado 2026-01-08 =====
fecha=2026-01-08: 510 filas, 1 archivo(s) parquet
Bronze total: 3467 filas
Silver: 2820 filas, 2820 pedido_id distintos, duplicados=0
v13 MERGE: numSourceRows=503, numTargetRowsInserted=0, numTargetRowsUpdated=503   <- última versión
ventas_por_region: 6 filas, suma total_ventas=2,402,312,700.0
top_productos: 10 filas, suma total_ventas=2,402,312,700.0
```

**¿Qué cambiarías en el pipeline para que este tipo de fallo no llegue
a producción sin que nadie se entere?**

- **Alerta activa:** un `on_failure_callback` en `validate_bronze` que
  mande el mensaje del `ValueError` (columna y porcentaje) a un canal de
  Slack o correo del equipo de datos. Hoy el fallo solo existe si alguien
  abre la UI de Airflow.
- **No reintentar los fallos de calidad:** el DAG tiene `retries=2` con
  5 minutos de espera, pensado para fallos transitorios (red, memoria).
  Un dato corrupto es determinista: los reintentos fallan igual y solo
  retrasan unos 10 minutos el aviso. Lanzaríamos
  `AirflowFailException` en los checks duros para fallar sin
  reintentar.
- **Avisar al consumidor:** si `validate_bronze` falla, Gold queda con
  el estado del día anterior y el reporte de la mañana mostraría cifras
  viejas como si fueran actuales. Publicaríamos junto a Gold la fecha de
  la última corrida exitosa (o un SLA en Airflow), para que el
  dashboard muestre que el dato está atrasado.
- **Cuarentena:** mover la partición rechazada a una ruta `quarantine/`
  con el motivo, para que alguien la revise y la reprocese con un
  backfill de esa fecha.

## Reflexión: Airflow vs. ejecutar el script manualmente

**¿Qué ganas con Airflow que no tenías con el script de Lab 1b?**
- **El pipeline se defiende solo:** en la Parte 4, `transform_silver` y
  `build_gold` no corrieron para el día corrupto, y Silver y Gold
  quedaron intactos. A mano, nada nos habría impedido correr el MERGE
  sobre ese día.
- **Reintentos con historial:** el `ingest_bronze` del 01-01 falló 3
  veces por permisos y quedó registrado con el log de cada intento. Con
  el script de Lab 1b, ese error se pierde en la terminal de quien lo
  corrió.
- **Backfill parametrizado:** cargar o recargar 7 días fue un solo
  comando, porque cada corrida recibe su `ds`.
- **Visibilidad:** el Grid muestra de un vistazo qué día y qué task
  falló, con su log, sin entrar al servidor.

**¿Qué te complica?**
- **Lentitud:** cada task levanta su propia JVM de Spark. Una corrida de
  ~400 filas tarda unos 2 minutos, y el backfill de 7 días tardó 14
  minutos (con `max_active_runs=1`). El script de Lab 1b procesaba todo
  en una sola sesión de Spark.
- **Los tasks no comparten memoria:** cada uno vuelve a leer de disco lo
  que escribió el anterior, y lo poco que pasa entre ellos (el conteo de
  Bronze) viaja por XCom.
- **Concurrencia:** la idempotencia de cada task no bastaba. Tuvimos que
  pensar en qué pasa cuando corren varias fechas a la vez y limitarlo
  con `max_active_runs=1`.
- **Fechas:** `ds`, el intervalo de datos y el momento real de ejecución
  son tres cosas distintas, todas en UTC. Es fácil equivocarse, como
  vimos con la corrida que arrancaría a las 19:00 hora de Colombia.
- **Entorno:** antes de correr el primer task hubo que resolver cuatro
  problemas del `docker-compose` y del Dockerfile dados (permisos del
  volumen, pytest ausente, ruta del CSV, descarga cortada de pyspark).
