# dag_design.md — Lab 4a: Diseño del DAG

**Curso:** ST1630 · **Semana:** S11 · **Fecha:** 2026-10-15
**Equipo:**
- Andrés Felipe Vélez Álvarez (`afveleza@eafit.edu.co`)
- Samuel Samper Cardona (`ssamperc@eafit.edu.co`)
- Sebastián Salazar Henao (`salazarh3@eafit.edu.co`)
- Hellen Yanes Doria (`hyanesd@eafit.edu.co`)

> Evidencia de soporte en `evidencia/` (conteos y log completo) y `capturas/`.

## Justificación del schedule

**schedule_interval elegido:** `@daily`

**Justificación:** El dato llega con granularidad diaria: cada corrida procesa
las ventas de un día completo (`ds`) y las integra a Silver/Gold. El equipo de
analítica de Nequi revisa estas métricas (ventas por región y mes, top
productos) una vez al día, típicamente en la mañana, con el día anterior ya
cerrado. Correrlo **cada hora** no aporta: el día todavía no ha terminado, se
reprocesaría la misma partición 24 veces y cada corrida levanta 4 sesiones de
Spark (en nuestra ejecución, cada corrida sin reintentos tomó entre 1:18 y
~1:35 min; *Min Run Duration* = 00:01:18 en el Grid). Correrlo **cada semana** es
demasiado lento para el negocio: un problema de datos o una caída en ventas se
vería con hasta 7 días de retraso, y cada corrida tendría que procesar 7 días
de golpe.

**¿Por qué `catchup=False`?** Con `catchup=False`, al activar el DAG Airflow
solo crea la corrida del intervalo más reciente y no rellena automáticamente
las fechas pasadas desde `start_date`. Si el DAG hubiera estado 30 días
apagado y lo activara con `catchup=True`, Airflow crearía de golpe **30
corridas** (una por cada día perdido) y las pondría en cola; en nuestro caso, con
`start_date=2026-01-01`, serían **~287 corridas**, la mayoría sobre fechas sin
datos (el dataset solo cubre 2026-01-01 a 2026-01-10), cada una levantando 4
JVM de Spark. Preferimos controlar el histórico de forma **explícita y acotada**
con `airflow dags backfill -s ... -e ...`, como hicimos para 2026-01-01 → 2026-01-07.
El trade-off es que, si el DAG se cae varios días, esos huecos **no** se
rellenan solos: alguien tiene que notarlo y lanzar el backfill (ver la sección
de alertas).

**Decisión adicional: `max_active_runs=1`.** Es el único cambio que hicimos
fuera de los `# TODO`: está en la definición del DAG, que venía dada. Lo
agregamos para que las fechas de un backfill se procesen **una a la vez, en
orden**, porque varias corridas en paralelo escriben sobre la misma tabla Silver:
1. **Primera escritura:** si 2026-01-01 y 2026-01-02 corrieran a la vez, ambas
   verían `DeltaTable.isDeltaTable(...) == False`, ambas entrarían a la rama
   `overwrite` y la última en terminar **borraría silenciosamente** los datos de
   la otra (sin ningún error en rojo).
2. **MERGE concurrentes:** Delta usa control de concurrencia optimista; dos
   MERGE simultáneos sobre la misma tabla pueden fallar por conflicto.
3. **Recursos:** cada task levanta su propia JVM de Spark; con ~7.3 GB de RAM
   asignados a Docker, varias en paralelo pueden agotar la memoria.

El costo es velocidad. El primer backfill de 7 fechas tardó ~15 min (la
primera corrida arrancó a las 06:35:59 y la última a las 06:49:44), pero
~5 min de eso fueron el `retry_delay` del `ingest_bronze` del 2026-01-01, que
falló una vez por permisos del volumen (por eso esa corrida dura 6:34 en el
Grid y la media sale en 2:08). El segundo backfill, sin reintentos, tardó
~10 min: solo entre el MERGE v7 (07:09:23) y el v13 (07:17:49) pasan 8:26. El historial de
Silver confirma el orden secuencial: v0 (WRITE, 01-01) y luego v1–v6 (MERGE)
cada ~1:25 min.

## Idempotencia — ¿qué significa y cómo la logramos?

**Definición:** Un pipeline es idempotente si ejecutarlo N veces con la
misma entrada produce el mismo resultado que ejecutarlo 1 vez.

**Cómo la implementamos en `ingest_bronze` (TODO 1):**

```python
df_dia.write.mode("overwrite").parquet(destino)   # destino = f"{BRONZE_DIR}/fecha={ds}"
```

Cada corrida escribe **solo la carpeta de su propio día** (`fecha=2026-01-02/`,
etc.) en modo `overwrite`. Si la task se reintenta o la fecha se vuelve a
backfillear, la carpeta de ese día se reemplaza completa con los mismos datos,
así que el resultado es el mismo sin importar cuántas veces corra. Con
`append`, cada reejecución agregaría otra copia: el 2026-01-02 pasaría de 399
filas a 798, y el error se propagaría a Silver y Gold sin que nada fallara.
El `overwrite` no borra los demás días porque la ruta de destino incluye
`fecha={ds}`: se sobrescribe únicamente la partición del día que se está
procesando, no la tabla Bronze completa (si escribiera sobre `BRONZE_DIR`
con `overwrite`, cada corrida sí borraría todo el histórico).

**Cómo la implementamos en `transform_silver` (TODO 3, el MERGE):**

```python
silver_table.alias("target").merge(
    df_transformado.alias("source"),
    "target.pedido_id = source.pedido_id",
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

A diferencia de Bronze, Silver es **una sola tabla con todos los días**, así que
no se puede usar `overwrite` (borraría los días anteriores) ni `append`
(duplicaría). El MERGE usa `pedido_id` como clave de negocio: si el pedido **ya
existe** en Silver, actualiza la fila con los valores que llegan; si **no
existe**, la inserta. Por eso, si Airflow reintenta la task después de que el
MERGE ya se ejecutó (por ejemplo, el contenedor se cae antes de registrar el
éxito), el segundo MERGE encuentra todos los `pedido_id` del día ya en Silver,
los "actualiza" con los mismos valores e inserta 0 filas nuevas. Con un
`append`, ese reintento habría insertado otra vez las 380 filas del 2026-01-02.
Además, Delta aplica el MERGE de forma atómica (todo o nada), así que un fallo
a mitad de camino no deja la tabla a medias.

**Prueba de idempotencia que realizamos:** Corrimos el backfill de 2026-01-01 a
2026-01-07, medimos Bronze/Silver/Gold con `contar_filas.py`, y luego volvimos a
correr el **mismo rango** con `--reset-dagruns -y`, que obliga a Airflow a
re-ejecutar las 28 tasks aunque ya estuvieran en verde (sin esa opción el
backfill se las salta y no se prueba nada). Después del segundo backfill los
conteos fueron idénticos en todas las capas: Bronze 2,957, Silver 2,820 (igual
al número de `pedido_id` distintos, es decir, cero duplicados) y Gold con la
misma suma de transacciones. Lo clave es el historial Delta de Silver: pasó de
la versión 6 a la **13**, es decir, se ejecutaron **7 MERGE nuevos** (uno por
fecha) y aun así Silver no creció ni una fila. Eso prueba que el trabajo sí se
rehízo y que el MERGE evitó la duplicación.

Comandos ejecutados (`contar_filas.py` está en `scripts/`; para correrlo lo
copiamos a `datos/`, que el contenedor monta en `/opt/airflow/data/raw`):

```bash
# 1er backfill
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline
docker compose exec airflow-scheduler python /opt/airflow/data/raw/contar_filas.py

# 2do backfill del MISMO rango, forzando re-ejecución de todas las tasks
docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline
docker compose exec airflow-scheduler python /opt/airflow/data/raw/contar_filas.py
```

Resultados reales (detalle completo en `evidencia/conteos_backfill1.txt` y `evidencia/conteos_backfill2.txt`):

| Capa | Tras 1er backfill | Tras 2do backfill | Diferencia |
|---|---:|---:|---:|
| Bronze (7 particiones) | 2,957 | 2,957 | 0 |
| Silver (filas) | 2,820 | 2,820 | 0 |
| Silver (`pedido_id` distintos) | 2,820 | 2,820 | 0 |
| Gold `ventas_por_region` (suma `n_transacciones`) | 2,820 | 2,820 | 0 |
| Versiones Delta de Silver | v0 (WRITE) + v1–v6 (MERGE) | v0–v13 (+7 MERGE) | se re-ejecutó sin duplicar |

```
Bronze por partición (idéntico en ambas corridas):
  fecha=2026-01-01: 325   fecha=2026-01-05: 311
  fecha=2026-01-02: 399   fecha=2026-01-06: 474
  fecha=2026-01-03: 529   fecha=2026-01-07: 521
  fecha=2026-01-04: 398   TOTAL BRONZE: 2957

Silver por fecha (idéntico en ambas corridas):
  2026-01-01: 316   2026-01-05: 296
  2026-01-02: 380   2026-01-06: 450
  2026-01-03: 498   2026-01-07: 503
  2026-01-04: 377   TOTAL SILVER: 2820 (= pedido_id distintos)
```

**Gold se recalcula completo en cada corrida.** `build_gold` lee **todo**
Silver y escribe las dos tablas con `mode("overwrite")`, así que su resultado
depende solo del estado actual de Silver. La suma de `n_transacciones` de
`ventas_por_region` es 2,820 en ambos backfills, igual al total de Silver.
El historial Delta de las dos tablas Gold lo confirma
(`evidencia/conteos_final.txt`): 15 versiones, v0–v14, **todas** `WRITE` en modo
`Overwrite`, una por cada corrida exitosa de `build_gold` (7 del primer
backfill, 7 del segundo y 1 de la recuperación del 2026-01-08). No hay ningún
MERGE ni append. Tampoco hay versión para el 2026-01-09, porque ese día
`build_gold` quedó en `upstream_failed`:

```
===== GOLD ventas_por_region: historial Delta =====
|0      |2026-10-15 06:42:25.582|WRITE    |Overwrite|
...
|13     |2026-10-15 07:18:14.835|WRITE    |Overwrite|
|14     |2026-10-15 08:51:38.605|WRITE    |Overwrite|

===== GOLD: top_productos =====
  filas: 10
|Smart TV 50 pulgadas   |1.9809694E9 |
|Tenis running          |2.158843E8  |
|Cafetera eléctrica     |1.633453E8  |
...
```

`top_productos` tiene 10 filas y no 20 porque el dataset solo tiene 10
productos distintos: el `limit(20)` es un tope, no una cantidad garantizada.
Con más productos en el catálogo, la tabla se cortaría en 20.

## Data quality checks implementados

| Check | Task donde vive | ¿Qué detecta? | ¿Qué hace si falla? |
|---|---|---|---|
| Nulos críticos | `validate_bronze` | Calcula, para `pedido_id`, `producto` y `total`, qué porcentaje de filas del día viene vacío, y lo registra en el log (p. ej. `Nulos en total: 51 de 510 (10.0%)`). Falla si **alguna** supera el 5%. | Lanza `ValueError` indicando la columna y el porcentaje. La task queda en rojo (tras los 2 reintentos) y `transform_silver`/`build_gold` quedan en `upstream_failed`: el día no entra a Silver ni a Gold (verificado en la Parte 4: Silver siguió en 2,820 filas). |
| Rango de monto | `validate_bronze` | Cuenta las filas con `total <= 0`. Basta **una** para fallar. | Lanza `ValueError` con el conteo exacto y bloquea el resto del pipeline igual que el anterior. |
| Cardinalidad de canal | `validate_bronze` | Cuenta los valores distintos de `canal` y avisa si hay más de 4 (los esperados: `app_movil`, `web`, `tienda_fisica`, `telefono`). | **No** lanza excepción: deja un `logging.warning` y el pipeline continúa. |

Los dos checks duros quedaron probados de dos formas. Con **pytest**, los 3
tests de `dags/tests/test_ventas_pipeline.py` pasan (`evidencia/pytest_output.txt`):

```
dags/tests/test_ventas_pipeline.py::test_validate_no_nulls_passes PASSED [ 33%]
dags/tests/test_ventas_pipeline.py::test_validate_nulls_fails PASSED     [ 66%]
dags/tests/test_ventas_pipeline.py::test_validate_negative_totals_fails PASSED [100%]

============================== 3 passed in 13.71s ==============================
```

Y con **datos reales corruptos** dentro del DAG, uno por check (ver la sección
"Fallo provocado"): nulos en `total` el 2026-01-08 y montos negativos el
2026-01-09.

**Por qué el umbral de nulos es 5% y no cero:** en datos reales es normal que
llegue algún nulo suelto (un error puntual de captura en la fuente), y tumbar
todo el reporte diario por 1 fila de 500 sería desproporcionado. Pero si los
nulos superan el 5%, ya no es ruido: indica que algo se rompió en la fuente.
Además, cada columna crítica tiene un riesgo concreto: un `total` nulo hace que
`F.sum("total")` en Gold **ignore** esa venta y reporte de menos sin ningún
error; y un `pedido_id` nulo **rompe el MERGE**, porque `null = null` no es
verdadero en SQL, así que esas filas nunca hacen *match* y se insertarían de
nuevo en cada reintento, duplicándose. El 5% es un compromiso razonable para
este dataset (en nuestros 7 días limpios hubo 0% de nulos), pero en producción lo
calibraríamos con el histórico real de la fuente.

**Por qué el rango de monto no tiene tolerancia:** en el modelo de negocio una
venta con total cero o negativo no existe (las devoluciones se marcan con la
columna `devuelto`, no con montos negativos). Un solo valor así indica un error
de cálculo o de captura, y como es dinero, distorsiona directamente las sumas
de Gold (`total_ventas`) que consume el negocio. Es preferible detener el día
y revisarlo que publicar un total incorrecto.

**Por qué la cardinalidad de canal solo avisa:** que aparezca un quinto canal
no significa necesariamente datos corruptos: puede ser un canal nuevo legítimo
(por ejemplo, si Nequi lanza ventas por WhatsApp). Si este check fallara duro,
un lanzamiento comercial normal dejaría a todo el equipo sin el reporte de
ventas del día. Con el *warning* el dato sigue fluyendo y alguien revisa si el
valor nuevo es legítimo (y se actualiza `CANALES_ESPERADOS_MAX`) o si es basura
(p. ej. `"Web "` o `"APP"` mal escritos). En nuestra ejecución no se disparó porque el
dataset tiene exactamente 4 canales.

**¿Qué check añadirías en un pipeline de producción real?**

1. **Volumen anormal del día.** Comparar el número de filas del día contra el
   histórico reciente (en nuestros datos, entre 311 y 529 filas por día). Si un día
   llega con, por ejemplo, menos del 50% del promedio de los últimos 7 días, es
   muy probable que la extracción de la fuente se haya cortado a la mitad. Los
   checks actuales **no** detectan esto: un archivo con 20 filas limpias pasaría
   todo y Gold reportaría una caída de ventas falsa.
2. **Duplicados de `pedido_id` dentro del mismo día.** Si la fuente envía el
   mismo pedido dos veces en la partición, el MERGE falla (Delta no permite que
   varias filas de origen hagan *match* con la misma fila destino) o, en la
   primera escritura, se insertan ambas. Fallaría duro si
   `count() != countDistinct("pedido_id")`.
3. **Consistencia del monto.** Verificar que `total == cantidad × precio_unit`
   (con una tolerancia por redondeo). Un `total` positivo pero mal calculado
   pasa el check de rango y aun así infla o reduce las ventas en Gold.

## Backfill — ¿qué es y cuándo se usa?

**Definición:** Un backfill es ejecutar el pipeline para un rango de **fechas
pasadas**, creando una corrida por cada intervalo como si el DAG hubiera estado
activo esos días. Cada corrida recibe su propia fecha lógica (`ds`) y procesa
solo los datos de ese día. Se usa para cargar el histórico cuando un DAG es
nuevo, para rellenar días que se perdieron porque el pipeline estuvo caído y
para reprocesar fechas después de corregir un bug. Como es muy común repetir un
backfill (porque falló a la mitad o porque se reprocesa un rango), el pipeline
tiene que ser idempotente; si no, cada repetición duplicaría datos.

**Comando que ejecutamos:**

```bash
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline
```

**¿Por qué es importante que `ingest_bronze` use `{{ ds }}` en vez de `datetime.now()`?**
`ds` es la fecha **lógica** de la corrida, no la fecha en que se ejecuta. En nuestro
backfill, las 7 corridas recibieron `ds` = 2026-01-01, 2026-01-02, … 2026-01-07,
aunque todas se ejecutaron el 2026-10-15. Si `ingest_bronze` usara
`datetime.now()`, las 7 corridas filtrarían el CSV por `fecha == 2026-10-15`,
encontrarían 0 filas y terminarían en verde con "nada que ingestar": el
backfill parecería exitoso pero no cargaría ningún dato (un fallo silencioso).
Y si hubiera datos para hoy, las 7 corridas escribirían en la misma carpeta
`fecha=2026-10-15`. Además, se perdería la idempotencia: correr la misma tarea
hoy y mañana daría resultados distintos, porque dependería del reloj y no de
la entrada. Con `ds` cada corrida es determinista: la misma fecha lógica
siempre procesa exactamente los mismos datos.

## Fallo provocado — log de `validate_bronze`

### Fallo 1 — nulos en `total` (2026-01-08)

Corrompimos el CSV dejando vacía la columna `total` en 51 de las 510 filas del
2026-01-08 (10%) y corrimos:

```bash
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-08 -e 2026-01-08 ventas_colombia_pipeline
```

Resultado: `ingest_bronze` = success · `validate_bronze` = failed (3 intentos) ·
`transform_silver` y `build_gold` = upstream_failed (no se ejecutaron).
Captura: `capturas/fallo_provocado_graph_2026-01-08.png`.

Log completo del intento 3 (`evidencia/log_validate_bronze_fallido_2026-01-08.log`):

```
[2026-10-15T07:36:34.112+0000] {local_task_job_runner.py:120} INFO - ::group::Pre task execution logs
[2026-10-15T07:36:34.153+0000] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-15T07:36:34.160+0000] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-15T07:36:34.162+0000] {taskinstance.py:2306} INFO - Starting attempt 3 of 3
[2026-10-15T07:36:34.174+0000] {taskinstance.py:2330} INFO - Executing <Task(PythonOperator): validate_bronze> on 2026-01-08 00:00:00+00:00
[2026-10-15T07:36:34.186+0000] {standard_task_runner.py:64} INFO - Started process 25890 to run task
[2026-10-15T07:36:34.188+0000] {standard_task_runner.py:90} INFO - Running: ['***', 'tasks', 'run', 'ventas_colombia_pipeline', 'validate_bronze', 'backfill__2026-01-08T00:00:00+00:00', '--job-id', '65', '--pool', 'default_pool', '--raw', '--subdir', 'DAGS_FOLDER/ventas_pipeline.py', '--cfg-path', '/tmp/tmpycd222te']
[2026-10-15T07:36:34.191+0000] {standard_task_runner.py:91} INFO - Job 65: Subtask validate_bronze
[2026-10-15T07:36:34.241+0000] {task_command.py:426} INFO - Running <TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [running]> on host d88c4be5a636
[2026-10-15T07:36:34.315+0000] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='st1630' AIRFLOW_CTX_DAG_ID='ventas_colombia_pipeline' AIRFLOW_CTX_TASK_ID='validate_bronze' AIRFLOW_CTX_EXECUTION_DATE='2026-01-08T00:00:00+00:00' AIRFLOW_CTX_TRY_NUMBER='3' AIRFLOW_CTX_DAG_RUN_ID='backfill__2026-01-08T00:00:00+00:00'
[2026-10-15T07:36:34.317+0000] {taskinstance.py:430} INFO - ::endgroup::
[2026-10-15T07:36:39.714+0000] {ventas_pipeline.py:164} INFO - Nulos en pedido_id: 0 de 510 (0.0%)
[2026-10-15T07:36:39.878+0000] {ventas_pipeline.py:164} INFO - Nulos en producto: 0 de 510 (0.0%)
[2026-10-15T07:36:40.183+0000] {ventas_pipeline.py:164} INFO - Nulos en total: 51 de 510 (10.0%)
[2026-10-15T07:36:40.486+0000] {taskinstance.py:441} INFO - ::group::Post task execution logs
[2026-10-15T07:36:40.487+0000] {taskinstance.py:2905} ERROR - Task failed with exception
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
  File "/opt/airflow/dags/ventas_pipeline.py", line 221, in validate_bronze
    check_nulos_criticos(df)
  File "/opt/airflow/dags/ventas_pipeline.py", line 166, in check_nulos_criticos
    raise ValueError(
ValueError: Check nulos críticos FALLÓ: la columna 'total' tiene 51 nulos de 510 filas (10.0%), supera el umbral de 5%.
[2026-10-15T07:36:40.501+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=ventas_colombia_pipeline, task_id=validate_bronze, run_id=backfill__2026-01-08T00:00:00+00:00, execution_date=20260108T000000, start_date=20261015T073634, end_date=20261015T073640
[2026-10-15T07:36:40.513+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 65 for task validate_bronze (Check nulos críticos FALLÓ: la columna 'total' tiene 51 nulos de 510 filas (10.0%), supera el umbral de 5%.; 25890)
[2026-10-15T07:36:40.545+0000] {local_task_job_runner.py:243} INFO - Task exited with return code 1
[2026-10-15T07:36:40.565+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-10-15T07:36:40.569+0000] {local_task_job_runner.py:222} INFO - ::endgroup::
```

Verificación posterior (`evidencia/conteos_tras_fallo_2026-01-08.txt`): Bronze
quedó con la partición `fecha=2026-01-08` (510 filas), pero **Silver siguió en
2,820 filas** y su última versión Delta siguió siendo la v13: el dato
corrupto nunca llegó a Silver ni a Gold.

### Fallo 2 — montos negativos (2026-01-09)

El fallo 1 solo prueba `check_nulos_criticos`. Para probar también
`check_rango_monto` con datos reales, volvimos `total` negativo en
6 de las 315 filas del 2026-01-09 (1.9%), **sin** dejar
nulos:

```bash
cd datos && python corromper_fecha.py 2026-01-09 0.02 negativo && cd ..
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-09 -e 2026-01-09 ventas_colombia_pipeline
```

Resultado: `ingest_bronze` = success · `validate_bronze` = failed ·
`transform_silver` y `build_gold` = upstream_failed. Esta vez el check de
nulos pasa (0%) y falla el de rango. Captura:
`capturas/fallo_rango_monto_graph_2026-01-09.png`. Log
(`evidencia/log_validate_bronze_fallido_2026-01-09.log`):

```
[2026-10-15T09:02:54.763+0000] {taskinstance.py:2306} INFO - Starting attempt 3 of 3
[2026-10-15T09:03:02.188+0000] {ventas_pipeline.py:164} INFO - Nulos en pedido_id: 0 de 315 (0.0%)
[2026-10-15T09:03:02.357+0000] {ventas_pipeline.py:164} INFO - Nulos en producto: 0 de 315 (0.0%)
[2026-10-15T09:03:02.554+0000] {ventas_pipeline.py:164} INFO - Nulos en total: 0 de 315 (0.0%)
[2026-10-15T09:03:02.556+0000] {ventas_pipeline.py:222} INFO - Check nulos críticos: OK
[2026-10-15T09:03:02.914+0000] {ventas_pipeline.py:181} INFO - Filas con total <= 0: 6
...
ValueError: Check rango de monto FALLÓ: 6 filas con total <= 0 (una venta no puede ser cero ni negativa).
```

Con solo 6 filas malas (1.9% del día), este check falla igual. Es la diferencia
con el de nulos: si el 1.9% de los `total` vinieran vacíos, el check de nulos
(umbral 5%) dejaría pasar el día. Silver no cambió: sigue sin filas del
2026-01-09 y su última versión siguió siendo la del 2026-01-08.

### Recuperación del 2026-01-08

Después de regenerar el CSV limpio (`python gen_dataset.py`), volvimos a
correr el 2026-01-08:

```bash
docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s 2026-01-08 -e 2026-01-08 ventas_colombia_pipeline
```

Las 4 tasks quedaron en verde (captura
`capturas/recuperacion_graph_2026-01-08.png`). El `overwrite` de `ingest_bronze` reemplazó la
partición corrupta `fecha=2026-01-08` (con sus 51 nulos) por la versión limpia,
y el MERGE agregó el día a Silver: **2,820 → 3,310 filas** (+490: las 510 del
día menos 20 devueltos), con 3,310 `pedido_id` distintos y una sola versión
nueva, la v14 (MERGE de las 08:51:13) (`evidencia/conteos_final.txt`). Gold
también se recalculó: la suma de `n_transacciones` pasó a 3,310.
Es la misma propiedad de idempotencia, ahora usada para reparar: rehacer un día
no deja rastros de la corrida anterior.

### Los reintentos no sirven para un error de datos

`validate_bronze` se intentó 3 veces (`retries=2`, `retry_delay` de 5 min), así
que la corrida tardó 11:09 en quedar en rojo, y los 3 intentos dieron
exactamente el mismo `ValueError`. Un reintento sirve para errores
**transitorios**, como el de permisos del volumen que sí se arregló al
reintentar `ingest_bronze`. Un dato corrupto, en cambio, no cambia entre
intentos: solo retrasa ~10 min la alerta. En producción le pondríamos
`retries=0` a `validate_bronze` (o lanzaríamos `AirflowFailException`, que
Airflow no reintenta) y dejaríamos los reintentos en las tasks que leen o
escriben archivos. No lo cambiamos en el código porque la definición del DAG
venía dada.

**¿Qué cambiarías en el pipeline para que este tipo de fallo no llegue
a producción sin que nadie se entere?** El check ya evita que el dato corrupto
llegue a Silver/Gold, pero en la Parte 4 nos enteramos del fallo solo porque estábamos
mirando la UI. En producción agregaríamos:

1. **Notificación automática al fallar:** un `on_failure_callback` en
   `default_args` que envíe un mensaje a Slack/Teams (o `email_on_failure=True`)
   con el DAG, la fecha (`ds`), la task y el mensaje del `ValueError` (por
   ejemplo, "columna 'total' con 51 nulos de 510 filas (10.0%)"), para que el
   equipo sepa qué revisar sin abrir Airflow.
2. **Alerta por retraso (SLA):** si el reporte de ventas tiene que estar listo
   a cierta hora (p. ej. 7:00 a. m.), configurar un SLA o un DAG de monitoreo que
   avise si a esa hora `build_gold` del día anterior no ha terminado. Esto cubre
   también fallos que no son de calidad (el contenedor caído, el scheduler
   detenido).
3. **Frescura visible en Gold:** agregar una columna `actualizado_hasta`
   (último `ds` procesado) en las tablas de Gold y mostrarla en el dashboard.
   Cuando `validate_bronze` falla, Gold no se actualiza pero sigue mostrando
   los datos del día anterior; sin esa columna, el negocio podría tomar
   decisiones con datos viejos creyendo que son de hoy.
4. **Escalar también los *warnings*:** enviar a un canal de menor prioridad
   el aviso de `check_cardinalidad_canal`, para que alguien decida si el canal
   nuevo es legítimo.

## Reflexión: Airflow vs. ejecutar el script manualmente

**¿Qué ganas con Airflow que no tenías con el script de Lab 1b?**
- **Ejecución automática y parametrizada por fecha:** el mismo código procesa
  cualquier día (`ds`) y corre solo cada día; en Lab 1b había que lanzarlo a
  mano y procesaba todo de una vez.
- **Reintentos automáticos:** lo vimos en la práctica. El `ingest_bronze` del
  2026-01-01 falló en el primer intento por un problema de permisos del volumen
  de Docker; después de corregirlo, Airflow lo reintentó solo y pasó (Try
  Number 2, captura `retry_ingest_bronze_2026-01-01.png`) sin necesidad de
  relanzar el backfill.
- **Dependencias y bloqueo ante datos malos:** si `validate_bronze` falla,
  Silver y Gold no se ejecutan (Parte 4). Con un script manual, la validación
  dependería de acordarse de hacerla a mano.
- **Backfill controlado** de rangos de fechas con un solo comando.
- **Visibilidad:** el Grid muestra de un vistazo qué días corrieron bien, qué
  falló y en qué task, con los logs de cada intento guardados.

**¿Qué te complica?**
- **Mucha infraestructura para un pipeline pequeño:** Docker, Postgres,
  scheduler, webserver y una imagen con Java + Spark. La primera construcción
  tardó bastante (solo `pyspark` pesa ~317 MB).
- **Problemas de entorno que no son del código:** el volumen `lab4a-data` se
  creó con dueño `root` y Spark no podía escribir (hubo que hacer `chown`);
  `pytest` no venía en la imagen; y el script del dataset genera el CSV en la
  carpeta desde donde se ejecuta, así que hay que correrlo dentro de `datos/`
  para que el contenedor lo vea.
- **Lentitud:** cada task levanta su propia sesión de Spark, así que cada
  corrida tomó ~1:20 min y un backfill de 7 días entre ~10 min (sin reintentos)
  y ~15 min (con uno). Con los reintentos de 5 minutos, el fallo provocado
  tardó 11:09 en quedar en rojo.
- **Hay que pensar en concurrencia y en conceptos como `catchup` o `ds`:**
  errores como correr fechas en paralelo o usar la fecha del sistema no
  producen un error visible, sino datos incorrectos.
