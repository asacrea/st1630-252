# dag_design.md — Lab 4a: Diseño del DAG

**Curso:** ST1630 · **Semana:** S11 · **Fecha:** 13/10/2026
**Estudiante:** Sebastian Andres Medina Cabezas - samedinac@eafit.edu.co


## Justificación del schedule

**schedule_interval elegido:** `@daily`

**Justificación:** → Se elige una ejecución diaria porque el equipo de Nequi o los analistas de negocio suelen consumir las tablas de la capa Gold para cierres contables diarios, reconciliación de transacciones al final del día y reportes de métricas agregadas (ej. total de ventas por canal o ciudad en el día). Un procesamiento batch diario es el estándar ideal para esto, ya que no se requiere analítica en tiempo real (streaming) para los reportes de cierre.

**¿Por qué `catchup=False`?** → Si el DAG hubiera estado apagado por 30 días y se encendiera con catchup=True, Airflow programaría y ejecutaría de golpe 30 corridas independientes (una por cada día atrasado) para "ponerse al día". Al usar catchup=False, le decimos a Airflow que ignore el historial perdido y solo ejecute el pipeline a partir de la fecha actual en adelante, evitando una sobrecarga masiva e innecesaria en el clúster si esos datos históricos no son requeridos con urgencia.

## Idempotencia — ¿qué significa y cómo la logré?

**Definición:** Un pipeline es idempotente si ejecutarlo N veces con la
misma entrada produce el mismo resultado que ejecutarlo 1 vez.

**Cómo la implementé en `ingest_bronze` (TODO 1):** → La implementé asegurándome de que Spark use el modo de escritura overwrite (sobreescritura) apuntando específicamente a la partición o directorio de la fecha lógica ({{ ds }}). De esta forma, si el DAG se corre dos veces para el mismo día, simplemente reemplaza los archivos de ese día sin duplicar la información.

**Cómo la implementé en `transform_silver` (TODO 3, el MERGE):** → Utilicé la operación MERGE INTO de Delta Lake comparando una llave primaria única (ej. id_transaccion). La condición indica que si el registro ya existe en la tabla Silver, se actualiza (o se ignora); y solo si no existe, se inserta. Si hubiera usado un append simple, correr el pipeline dos veces habría duplicado todos los registros de ese día.

**Prueba de idempotencia que realicé:** → Para comprobar la idempotencia de forma rigurosa, desarrollé y ejecuté un script de validación en PySpark que lee directamente el estado y el historial de la tabla Delta. La prueba confirma dos cosas fundamentales:

El historial de Delta Lake registra que la inserción del primer día fue un WRITE (versión 0), y las ejecuciones de los días posteriores (o reprocesamientos de la misma fecha) utilizaron MERGE de forma correcta.

El conteo total de filas en la capa Silver coincide de manera exacta con el conteo de pedido_id únicos distintos. Esto demuestra matemáticamente que la operación no generó registros duplicados al procesar o reprocesar datos, validando la idempotencia.

1 try

```
Bronze por fecha: 325, 399, 529, 398, 311, 474, 521, 510                        
Bronze total: 3467
Silver total: 2820                                                              
pedido_id distintos en Silver: 2820                                             
Gold ventas_por_region: 6 filas; suma total_ventas: $2.402.312.700
Gold top_productos: 10 filas
Historial Delta Silver: versión 0 WRITE; versión 1 MERGE; versión 2 MERGE; versión 3 MERGE; versión 4 MERGE; versión 5 MERGE; versión 6 MERGE; versión 7 MERGE; versión 8 MERGE
```

2 try

```
Bronze por fecha: 325, 399, 529, 398, 311, 474, 521, 510                        
Bronze total: 3467
Silver total: 2820                                                              
pedido_id distintos en Silver: 2820                                             
Gold ventas_por_region: 6 filas; suma total_ventas: $2.402.312.700
Gold top_productos: 10 filas
Historial Delta Silver: versión 0 WRITE; versión 1 MERGE; versión 2 MERGE; versión 3 MERGE; versión 4 MERGE; versión 5 MERGE; versión 6 MERGE; versión 7 MERGE; versión 8 MERGE
```

## Data quality checks implementados

| Check | Task donde vive | ¿Qué detecta? | ¿Qué hace si falla? |
|---|---|---|---|
| Nulos críticos | `validate_bronze` | → Filas que tienen valores nulos (NULL) en columnas esenciales para el negocio (ej. id, monto, fecha). | →Levanta una excepción (falla la tarea) para detener el pipeline y evitar que datos corruptos lleguen a Silver/Gold. |
| Rango de monto | `validate_bronze` | → Transacciones con montos negativos, en cero, o fuera de un rango lógico/permitido. | → Levanta una excepción, fallando la tarea inmediatamente. |
| Cardinalidad de canal | `validate_bronze` | → Valores en la columna categórica (ej. método de pago) que no están dentro de la lista de valores esperados. | → Solo imprime un Warning (advertencia) en los logs. Esto tiene sentido porque un nuevo canal podría ser una actualización del sistema origen, y no queremos tumbar todo el pipeline por un dato que quizás sea legítimo, sino alertar para revisar. |

**¿Qué check añadirías en un pipeline de producción real?** → Verificación de duplicados estrictos: Un check que valide que el id_transaccion sea 100% único en el lote diario antes de pasarlo a Silver.

Integridad Referencial: Un check que valide que los IDs de los usuarios/clientes en las transacciones realmente existan en nuestra tabla de dimensión de clientes (o que cumplan con un formato regex específico).

## Backfill — ¿qué es y cuándo se usa?

**Definición:** → Es el proceso de ejecutar un DAG para periodos de tiempo en el pasado. Se usa cuando se crea un nuevo pipeline y necesitamos procesar datos históricos, o cuando se arregla un bug en el código y necesitamos reprocesar días anteriores para corregir la información.

**Comando que ejecuté:** → docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline

**¿Por qué es importante que `ingest_bronze` use `{{ ds }}` en vez de,
por ejemplo, "la fecha de hoy" (`datetime.now()`)?** → Si usamos datetime.now(), sin importar qué fecha histórica queramos procesar, el código siempre leería los datos del día en que se está ejecutando el comando (hoy). Al usar la macro {{ ds }} (fecha lógica de ejecución), Airflow inyecta dinámicamente la fecha pasada correspondiente al backfill (ej. "2026-01-02"), permitiendo que el pipeline procese correctamente los archivos de ese día en particular.

## Fallo provocado — log de `validate_bronze`

Pega aquí el log completo de la task cuando falló (Parte 4 del README):

Fallo Especifico

```
raise ValueError(f"Fallo de calidad de datos: la columna '{columna}' tiene un {porcentaje:.2%} de nulos, "
ValueError: Fallo de calidad de datos: la columna 'total' tiene un 8.04% de nulos, superando el umbral permitido del 5.00%.
```

Log completo

```
1934a0a8871a
*** Found local files:
***   * /opt/airflow/logs/dag_id=ventas_colombia_pipeline/run_id=backfill__2026-01-08T00:00:00+00:00/task_id=validate_bronze/attempt=1.log
[2026-10-13, 15:23:52 UTC] {local_task_job_runner.py:120} ▼ Pre task execution logs
[2026-10-13, 15:23:52 UTC] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-13, 15:23:52 UTC] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-13, 15:23:52 UTC] {taskinstance.py:2306} INFO - Starting attempt 1 of 3
[2026-10-13, 15:23:52 UTC] {taskinstance.py:2330} INFO - Executing <Task(PythonOperator): validate_bronze> on 2026-01-08 00:00:00+00:00
[2026-10-13, 15:23:52 UTC] {standard_task_runner.py:64} INFO - Started process 30047 to run task
[2026-10-13, 15:23:52 UTC] {standard_task_runner.py:90} INFO - Running: ['***', 'tasks', 'run', 'ventas_colombia_pipeline', 'validate_bronze', 'backfill__2026-01-08T00:00:00+00:00', '--job-id', '108', '--pool', 'default_pool', '--raw', '--subdir', 'DAGS_FOLDER/ventas_pipeline.py', '--cfg-path', '/tmp/tmpdwcwariz']
[2026-10-13, 15:23:52 UTC] {standard_task_runner.py:91} INFO - Job 108: Subtask validate_bronze
[2026-10-13, 15:23:52 UTC] {task_command.py:426} INFO - Running <TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [running]> on host 1934a0a8871a
[2026-10-13, 15:23:52 UTC] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='st1630' AIRFLOW_CTX_DAG_ID='ventas_colombia_pipeline' AIRFLOW_CTX_TASK_ID='validate_bronze' AIRFLOW_CTX_EXECUTION_DATE='2026-01-08T00:00:00+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='backfill__2026-01-08T00:00:00+00:00'
[2026-10-13, 15:23:52 UTC] {taskinstance.py:430} ▲▲▲ Log group end
[2026-10-13, 15:24:06 UTC] {taskinstance.py:441} ▼ Post task execution logs
[2026-10-13, 15:24:06 UTC] {taskinstance.py:2905} ERROR - Task failed with exception
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
  File "/opt/airflow/dags/ventas_pipeline.py", line 178, in validate_bronze
    check_nulos_criticos(df)
  File "/opt/airflow/dags/ventas_pipeline.py", line 135, in check_nulos_criticos
    raise ValueError(f"Fallo de calidad de datos: la columna '{columna}' tiene un {porcentaje:.2%} de nulos, "
ValueError: Fallo de calidad de datos: la columna 'total' tiene un 8.04% de nulos, superando el umbral permitido del 5.00%.
[2026-10-13, 15:24:06 UTC] {taskinstance.py:1206} INFO - Marking task as UP_FOR_RETRY. dag_id=ventas_colombia_pipeline, task_id=validate_bronze, run_id=backfill__2026-01-08T00:00:00+00:00, execution_date=20260108T000000, start_date=20261013T152352, end_date=20261013T152406
[2026-10-13, 15:24:06 UTC] {standard_task_runner.py:110} ERROR - Failed to execute job 108 for task validate_bronze (Fallo de calidad de datos: la columna 'total' tiene un 8.04% de nulos, superando el umbral permitido del 5.00%.; 30047)
[2026-10-13, 15:24:06 UTC] {local_task_job_runner.py:243} INFO - Task exited with return code 1
[2026-10-13, 15:24:06 UTC] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2026-10-13, 15:24:06 UTC] {local_task_job_runner.py:222} ▲▲▲ Log group end
```

**¿Qué cambiarías en el pipeline para que este tipo de fallo no llegue
a producción sin que nadie se entere?** →Configuraría notificaciones automáticas utilizando el parámetro on_failure_callback de Airflow para enviar alertas directamente a un canal de Slack/Teams del equipo de datos o por correo electrónico. Además, integraría métricas de calidad de datos en un dashboard de monitoreo (como Grafana o Datadog) para tener visibilidad en tiempo real de la salud del pipeline.

## Reflexión: Airflow vs. ejecutar el script manualmente

**¿Qué ganas con Airflow que no tenías con el script de Lab 1b?**
→ Gano automatización total (scheduling), orquestación visual de dependencias (saber qué falla y por qué), mecanismos de reintento automático (retries), aislamiento de tareas, un registro histórico visual fácil de auditar, y la capacidad de hacer backfills paramétricos de manera nativa y organizada.

**¿Qué te complica?** → La curva de aprendizaje inicial es mucho más alta. Desplegar la infraestructura (Docker, contenedores, scheduler, webserver) y gestionar configuraciones, variables y conexiones es mucho más complejo que simplemente correr un script de Python aislado en la terminal. Además, depurar errores de concurrencia requiere más atención al diseño del entorno.