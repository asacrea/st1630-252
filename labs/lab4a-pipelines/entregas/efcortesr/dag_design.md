# Lab 4a · Diseño y verificación del DAG

**Curso:** ST1630 · **Semana:** S11 · **Fecha:** 2026-10-09
**Equipo:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio y Mariana Sanchez
**Correos institucionales:** pendientes de confirmar

## Justificación del schedule

El DAG usa `@daily` para entregar cada mañana las ventas del día anterior en las tablas Gold. El equipo puede consultar un corte diario estable por región, mes y producto. `catchup=False` evita que, al activar el DAG tras 30 días de inactividad, Airflow lance automáticamente las 30 corridas atrasadas. Para recuperar fechas históricas se usa un backfill explícito y controlado.

Se fijó `max_active_runs=1` para que los días del backfill escriban Silver y recalculen Gold en secuencia. Así dos corridas no intentan modificar simultáneamente las mismas tablas Delta.

## Idempotencia

Un pipeline es idempotente cuando repetirlo con la misma entrada deja el mismo estado final. Bronze escribe cada fecha en `.../fecha={{ ds }}` con `overwrite`: repetir el día reemplaza su partición sin añadir filas ni borrar las demás. Silver usa `MERGE` por `pedido_id`: un pedido existente se actualiza y uno nuevo se inserta. Si Airflow reintenta después de que Delta confirmó el `MERGE`, la segunda ejecución encuentra las mismas claves y no crea copias. Un `append` insertaría de nuevo todos los pedidos.

Gold se recalcula desde todo Silver y se sobrescribe en cada corrida; por ello refleja el estado acumulado actual. En una fecha sin datos y sin tabla Silver previa, Gold se omite.

**Primera ejecución del rango 2026-01-01 a 2026-01-07:** las siete corridas y las 28 tasks terminaron en `success`. Los conteos se leyeron directamente de los archivos Parquet y las tablas Delta del volumen del lab:

```text
Bronze por fecha: 325, 399, 529, 398, 311, 474, 521
Bronze total: 2.957
Silver total: 2.820
pedido_id distintos en Silver: 2.820
Gold ventas_por_region: 6 filas; suma total_ventas: $2.402.312.700
Gold top_productos: 10 filas
Historial Delta Silver: versión 0 WRITE; versiones 1–6 MERGE
```

**Segunda ejecución del mismo rango:** se usó `airflow dags backfill --reset-dagruns -y -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline`. De nuevo terminaron siete corridas y 28 tasks en `success`. La lectura posterior de las tablas dio:

```text
Bronze por fecha: 325, 399, 529, 398, 311, 474, 521
Bronze total: 2.957 (cambio: 0)
Silver total: 2.820 (cambio: 0)
pedido_id distintos en Silver: 2.820 (cambio: 0)
Gold ventas_por_region: 6 filas; suma total_ventas: $2.402.312.700 (cambio: 0)
Gold top_productos: 10 filas (cambio: 0)
Historial Delta Silver: versión 0 WRITE; versiones 1–13 MERGE
```

El historial registra siete operaciones `MERGE` adicionales, pero ninguna agregó filas duplicadas. `Bronze` conservó exactamente las siete particiones y sus conteos. Esta prueba compara el estado almacenado, no solo que el DAG haya terminado en verde.

## Controles de calidad

| Control | Condición | Efecto |
|---|---|---|
| Nulos críticos | Alguna de `pedido_id`, `producto` o `total` supera el 5 % de nulos de la partición | `ValueError` con columna, conteo y porcentaje; bloquea Silver y Gold |
| Rango de monto | Al menos una fila tiene `total <= 0` | `ValueError` con conteo; bloquea Silver y Gold |
| Cardinalidad de canal | Más de cuatro valores distintos en `canal` | Advertencia en el log; la corrida continúa porque puede ser un canal nuevo válido |

El umbral de 5 % distingue una anomalía significativa de algunos faltantes aislados en este dataset de práctica. Un monto no positivo no representa una venta válida y se rechaza desde la primera aparición. En producción añadiríamos un control de unicidad de `pedido_id` por partición y otro que contraste `total` con `cantidad * precio_unit`, admitiendo una tolerancia definida para redondeos y descuentos.

Los tres tests de `dags/tests/test_ventas_pipeline.py` pasaron. En el log de `validate_bronze` del 2026-01-07, segundo backfill, Airflow registró `Check nulos críticos: OK`, `Check rango de monto: OK`, `Check cardinalidad de canal: completado` y finalmente `Marking task as SUCCESS`.


## Backfill

Un backfill ejecuta el DAG para fechas históricas usando la fecha lógica de cada corrida. Se usa para recuperar días que no se procesaron o para reprocesarlos después de una corrección. `ingest_bronze` filtra el CSV con `ds`; si usara la fecha actual del sistema, las siete corridas históricas leerían el día equivocado.

```bash
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline
```

**Resultado:** siete corridas exitosas, cada una con `ingest_bronze`, `validate_bronze`, `transform_silver` y `build_gold` en `success`. Las capturas de Grid, Graph y del log de validación están en `capturas/`.

## Fallo provocado

Se editó solo la copia local del CSV para dejar vacío `total` en 31 de las 510 filas del 2026-01-08 (6,08 %). `ingest_bronze` terminó correctamente y `validate_bronze` detectó el problema en su primer intento; Airflow agotó los tres intentos configurados y marcó la task como `FAILED`. `transform_silver` y `build_gold` quedaron en `upstream_failed`, sin ejecutarse para esa fecha. Después de la prueba se regeneró el CSV con la semilla fija.

La lectura posterior mostró Bronze con 510 filas adicionales para el 8 de enero, pero Silver siguió en 2.820 pedidos únicos y Gold conservó el total de $2.402.312.700. Esto confirma que la validación bloqueó el avance de la partición corrupta.

```text
[2026-10-09T18:49:11.660+0000] {local_task_job_runner.py:120} INFO - ::group::Pre task execution logs
[2026-10-09T18:49:11.696+0000] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-09T18:49:11.703+0000] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-09T18:49:11.704+0000] {taskinstance.py:2306} INFO - Starting attempt 3 of 3
[2026-10-09T18:49:11.716+0000] {taskinstance.py:2330} INFO - Executing <Task(PythonOperator): validate_bronze> on 2026-01-08 00:00:00+00:00
[2026-10-09T18:49:11.722+0000] {standard_task_runner.py:64} INFO - Started process 22098 to run task
[2026-10-09T18:49:11.724+0000] {standard_task_runner.py:90} INFO - Running: ['***', 'tasks', 'run', 'ventas_colombia_pipeline', 'validate_bronze', 'backfill__2026-01-08T00:00:00+00:00', '--job-id', '66', '--pool', 'default_pool', '--raw', '--subdir', 'DAGS_FOLDER/ventas_pipeline.py', '--cfg-path', '/tmp/tmpwxgccr_u']
[2026-10-09T18:49:11.726+0000] {standard_task_runner.py:91} INFO - Job 66: Subtask validate_bronze
[2026-10-09T18:49:11.771+0000] {task_command.py:426} INFO - Running <TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [running]> on host 314c9608e34c
[2026-10-09T18:49:11.846+0000] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='st1630' AIRFLOW_CTX_DAG_ID='ventas_colombia_pipeline' AIRFLOW_CTX_TASK_ID='validate_bronze' AIRFLOW_CTX_EXECUTION_DATE='2026-01-08T00:00:00+00:00' AIRFLOW_CTX_TRY_NUMBER='3' AIRFLOW_CTX_DAG_RUN_ID='backfill__2026-01-08T00:00:00+00:00'
[2026-10-09T18:49:11.847+0000] {taskinstance.py:430} INFO - ::endgroup::
[2026-10-09T18:49:18.846+0000] {taskinstance.py:441} INFO - ::group::Post task execution logs
[2026-10-09T18:49:18.847+0000] {taskinstance.py:2905} ERROR - Task failed with exception
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
  File "/opt/airflow/dags/ventas_pipeline.py", line 151, in validate_bronze
    check_nulos_criticos(df)
  File "/opt/airflow/dags/ventas_pipeline.py", line 104, in check_nulos_criticos
    raise ValueError(
ValueError: La columna total tiene 6.08% de nulos (31/510); supera el umbral de 5%.
[2026-10-09T18:49:18.859+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=ventas_colombia_pipeline, task_id=validate_bronze, run_id=backfill__2026-01-08T00:00:00+00:00, execution_date=20260108T000000, start_date=20261009T184911, end_date=20261009T184918
[2026-10-09T18:49:18.871+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 66 for task validate_bronze (La columna total tiene 6.08% de nulos (31/510); supera el umbral de 5%.; 22098)
[2026-10-09T18:49:18.916+0000] {local_task_job_runner.py:243} INFO - Task exited with return code 1
[2026-10-09T18:49:18.936+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-10-09T18:49:18.941+0000] {local_task_job_runner.py:222} INFO - ::endgroup::
```

En producción agregaríamos una alerta automática al canal de operación cuando `validate_bronze` falle, con fecha lógica, columna y porcentaje. Un tablero de corridas mostraría el último día Gold actualizado para detectar también la ausencia de ejecución.

## Airflow frente a la ejecución manual

Airflow aporta calendario, dependencias visibles, reintentos, backfill y un registro por task y fecha. La ejecución manual del Lab 1b requería recordar el orden y observar cada comando por separado. El costo es operar Airflow y PostgreSQL, mantener la imagen con Spark/Delta y vigilar el estado de las corridas; además, los reintentos obligan a diseñar escrituras idempotentes.
