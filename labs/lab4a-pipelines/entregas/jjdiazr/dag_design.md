# dag_design.md — Lab 4a: Diseño del DAG

**Curso:** ST1630 Sistemas Intensivos en Datos · **Semana:** S11 · **Fecha:** 2026-10-22  
**Estudiante:** Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co  

---

## Justificación del schedule

**schedule_interval elegido:** `@daily`

**Justificación:**  
El pipeline orquesta un procesamiento de datos tipo *batch* diario. En un contexto como el de Nequi o comercio electrónico, las transacciones del día se consolidan al cierre de la medianoche. Los equipos de analítica, producto y finanzas consumen los tableros e indicadores en la capa Gold al inicio de la jornada laboral para la toma de decisiones. Un intervalo diario es el balance adecuado entre frescura del dato y el costo computacional de levantar jobs de PySpark y recalcular agregaciones sobre tablas Delta.

**¿Por qué `catchup=False`?**  
`catchup=False` le indica a Airflow que, al encender o activar el DAG por primera vez, no intente disparar automáticamente ejecuciones históricas para cada día transcurrido desde `start_date` (2026-01-01).  
Si se hubiera configurado `catchup=True` tras 30 días de inactividad, Airflow intentaría lanzar 30 `dag_runs` en paralelo. En un clúster o contenedor local, esto provocaría una saturación extrema de memoria RAM (al levantar decenas de instancias concurrentes de la JVM de Spark) y contención/deadlocks sobre las tablas de metadatos en PostgreSQL. Con `catchup=False`, el DAG solo planifica hacia adelante y cualquier carga histórica se controla de forma deliberada y secuencial mediante `airflow dags backfill`.

---

## Idempotencia — ¿qué significa y cómo la logré?

**Definición:**  
Un pipeline es idempotente si ejecutarlo $N$ veces consecutivas con los mismos datos de entrada produce exactamente el mismo estado final en el almacenamiento que ejecutarlo una sola vez ($f(f(x)) = f(x)$), sin generar registros duplicados ni inconsistencias.

**Cómo la implementé en `ingest_bronze` (TODO 1):**  
Se utilizó la escritura de PySpark en formato Parquet con modo de guardado `"overwrite"` acotado estrictamente al directorio de la partición del día:  
```python
destino = f"{BRONZE_DIR}/fecha={ds}"
df_dia.write.mode("overwrite").parquet(destino)
```
Al sobrescribir únicamente la subcarpeta `fecha={ds}`, si Airflow reintenta la tarea por fallo de red o si se corre un backfill repetido, los archivos Parquet de esa fecha específica se reemplazan limpiamente sin duplicar filas y sin tocar las particiones de las demás fechas ya ingeridas.

**Cómo la implementé en `transform_silver` (TODO 3, el MERGE):**  
En Silver se utilizó la operación `MERGE` de Delta Lake indexada por la clave única de negocio `pedido_id`:
```python
silver_table.alias("target").merge(
    df_transformado.alias("source"),
    "target.pedido_id = source.pedido_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```
Si se utilizara un simple `.write.mode("append")`, cada reintento de la tarea insertaría nuevamente todas las filas en la tabla Delta, duplicando ventas e inflando artificialmente las métricas.  
Con `MERGE`, si la tarea falla y se reejecuta, o si el contenedor se apaga justo antes de confirmar el éxito a Airflow, la segunda corrida encuentra que los `pedido_id` ya existen en el destino (`target`), por lo que ejecuta `whenMatchedUpdateAll()` actualizando los registros existentes en lugar de duplicarlos.

**Prueba de idempotencia que realicé:**  
Se ejecutó dos veces consecutivas el procesamiento para la fecha `2026-01-01`.  
- En la primera ejecución, Silver se inicializó con las filas no devueltas del día 2026-01-01.
- En la segunda ejecución, `MERGE` procesó las mismas filas y actualizó en lugar de insertar.
- Al completar las 7 fechas del backfill (2026-01-01 a 2026-01-07), el conteo total de filas verificado mediante Delta Lake fue:

```text
Total filas en Silver: 2,820
Total filas en Gold Region: 6
Total filas en Gold Top Productos: 10
```
El conteo no creció ni se alteró al reejecutar fechas, demostrando que Silver permanece libre de duplicados.

---

## Data quality checks implementados

| Check | Task donde vive | ¿Qué detecta? | ¿Qué hace si falla? |
|---|---|---|---|
| Nulos críticos | `validate_bronze` | Revisa `pedido_id`, `producto` y `total`. Calcula si el porcentaje de valores nulos supera el 5% (`UMBRAL_NULOS = 0.05`). | Lanza `ValueError` con el porcentaje exacto y detiene el DAG (marcando la tarea en rojo), bloqueando las capas downstream (Silver y Gold). |
| Rango de monto | `validate_bronze` | Revisa filas donde `total <= 0` (ventas con monto negativo o cero que carecen de lógica de negocio). | Lanza `ValueError` si encuentra al menos 1 fila inválida, fallando inmediatamente la tarea. |
| Cardinalidad de canal | `validate_bronze` | Cuenta los valores distintos en `canal` y comprueba si supera 4 (`CANALES_ESPERADOS_MAX`). | Emite un `logger.warning` en los logs pero **no lanza excepción**. Esto permite que el pipeline continúe, ya que un canal nuevo puede ser una estrategia comercial legítima (e.g. nuevo canal de WhatsApp) y no necesariamente corrupción de datos. |

**¿Qué checks añadirías en un pipeline de producción real?**  
1. **Unicidad de clave primaria en Bronze/Silver:** Validar que `df.count() == df.select("pedido_id").distinct().count()`. Si una misma partición diaria trae duplicados en `pedido_id`, indica fallos en el sistema transaccional origen.
2. **Consistencia de cálculo numérico:** Verificar que `abs(total - (cantidad * precio_unit)) < 0.01`. Si el total facturado difiere del producto de cantidad por precio unitario, existe un error de redondeo o corrupción en la pasarela de pagos.

---

## Backfill — ¿qué es y cuándo se usa?

**Definición:**  
El backfill es el proceso de ejecutar un DAG programado sobre fechas lógicas del pasado. Se utiliza al poner en marcha un pipeline nuevo que requiere procesar datos históricos, al recalcular métricas tras un cambio en la lógica de negocio, o al recuperarse de caídas prolongadas de los sistemas fuente.

**Comando que ejecuté:**  
```bash
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-01 -e 2026-01-07 -y ventas_colombia_pipeline
```

**¿Por qué es importante que `ingest_bronze` use `{{ ds }}` en vez de `datetime.now()`?**  
`{{ ds }}` es la fecha lógica de ejecución que Airflow inyecta en el contexto de la tarea (por ejemplo `"2026-01-03"`).  
Si el script usara `datetime.now()`, tomaría la fecha del reloj del sistema operativo (22 de octubre de 2026). Al hacer un backfill histórico, el pipeline intentaría buscar datos de hoy en lugar de los datos del día histórico correspondiente, haciendo imposible rellenar datos del pasado o reproducir resultados deterministas.

---

## Fallo provocado — log de `validate_bronze`

Para la fecha `2026-01-08` se introdujeron intencionalmente valores vacíos en la columna `total` en 40 de las 510 filas (7.84% de nulos, superando el umbral de 5.00%).

Log real extraído de la ejecución:

```text
    File "/opt/airflow/dags/ventas_pipeline.py", line 152, in check_nulos_criticos
      raise ValueError(
ValueError: Check nulos críticos falló en columna 'total': 7.84% de nulos (40/510 filas) supera el umbral permitido de 5.00%
[2026-10-22T18:33:34.268+0000] {taskinstance.py:1206} INFO - Marking task as FAILED. dag_id=ventas_colombia_pipeline, task_id=validate_bronze, run_id=backfill__2026-01-08T00:00:00+00:00, execution_date=20260108T000000, start_date=20261022T183327, end_date=20261022T183334
[2026-10-22T18:33:34.294+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 69 for task validate_bronze (Check nulos críticos falló en columna 'total': 7.84% de nulos (40/510 filas) supera el umbral permitido de 5.00%; 17971)
[2026-10-22T18:33:34.343+0000] {local_task_job_runner.py:243} INFO - Task exited with return code 1
[2026-10-22T18:33:34.371+0000] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
```

Como resultado, `validate_bronze` quedó marcada en rojo (`failed`) y las tareas dependientes `transform_silver` y `build_gold` pasaron a estado `upstream_failed`, garantizando que ningún dato corrupto alcanzara las tablas de analítica.

**¿Qué cambiarías en el pipeline para que este tipo de fallo no llegue a producción sin que nadie se entere?**  
1. Configurar un `on_failure_callback` en el DAG o en la tarea que envíe una alerta inmediata con el mensaje de error a un canal de Slack/Microsoft Teams del equipo de Data Engineering o dispare una incidencia en PagerDuty.
2. Integrar un exportador de métricas (StatsD / Prometheus) conectado a un dashboard de Grafana para monitorear la tasa de éxito de los checks de calidad y alertar proactivamente sobre degradación en las fuentes.

---

## Reflexión: Airflow vs. ejecutar el script manualmente

**¿Qué ganas con Airflow que no tenías con el script de Lab 1b?**  
- **Automatización y reintentos:** Se desliga la ejecución de la presencia humana; si una tarea sufre una caída transitoria, Airflow la reintenta automáticamente con retroceso programado (`retries=2`, `retry_delay=5min`).
- **Control de dependencias estricto:** Si `validate_bronze` falla, Airflow corta automáticamente el flujo hacia Silver y Gold sin necesidad de manejar complejas sentencias de salida manual en Bash.
- **Trazabilidad y observabilidad:** La UI permite inspeccionar el estado histórico de cada ejecución, consultar logs centralizados de cada tarea y saber exactamente qué falló y cuándo.

**¿Qué te complica?**  
- **Sobrecarga de infraestructura:** Requiere mantener y coordinar múltiples componentes (base de datos PostgreSQL de metadatos, Scheduler, Webserver, volúmenes Docker y dependencias de permisos de usuario como UID/GID).
- **Curva de depuración:** Depurar errores de concurrencia o contención de bloqueos (como los deadlocks de base de datos durante backfills paralelos masivos) es considerablemente más complejo que depurar un script Python monolítico local.
