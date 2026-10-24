# dag_design.md — Lab 4a: Diseño del DAG

**Curso:** ST1630 Sistemas Intensivos en Datos · **Semana:** S11 · **Fecha:** 2026-10-14  
**Estudiantes:**
- Mateo Sanz Medina (`msanzm@eafit.edu.co`)
- Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`)
- Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

---

## 1. Justificación del schedule

### `schedule_interval` elegido: `@daily`

**Justificación:**  
En el ecosistema analítico de una fintech/retail como Nequi, el flujo transaccional se consolida por ciclos diarios contables y comerciales. Los tableros ejecutivos y analíticos en la capa Gold (métricas agregadas de facturación por región y ranking de productos más vendidos) son consumidos primordialmente por los equipos de finanzas, operaciones y analítica de negocio a primera hora de la mañana para la toma de decisiones estratégicas.

Configurar el pipeline con frecuencia diaria (`@daily`) permite procesar la totalidad de las transacciones correspondientes al día $D$ durante la ventana batch nocturna (cuando la actividad transaccional OLTP de los usuarios disminuye considerablemente), evitando la competencia por recursos de I/O y cómputo con la plataforma transaccional activa.

### ¿Por qué `catchup=False`?
Si el DAG se configurara con `catchup=True` y hubiera estado inactivo o en pausa durante un periodo prolongado (por ejemplo, 30 días tras su fecha de inicio `start_date`), al activarse en Airflow el `Scheduler` intentaría planificar y disparar inmediatamente **30 DAG runs simultáneos** (uno por cada día transcurrido) para "ponerse al día". 

En un entorno local o de clúster con recursos acotados, disparar 30 DAG runs implicaría levantar docenas de `SparkSession` concurrentes, saturando la memoria RAM y CPU del clúster, provocando bloqueos de archivos en el filesystem y generando potenciales colisiones de escritura concurrente en la tabla Delta Lake durante la operación `MERGE`. 

Con `catchup=False`, Airflow únicamente agenda la corrida más reciente al momento de activar el DAG, protegiendo la infraestructura. Cuando se requiere reprocesar o cargar periodos históricos, la buena práctica exige hacerlo de forma explícita, controlada y secuencial mediante la utilidad de **Backfill por línea de comandos** (`airflow dags backfill`).

---

## 2. Idempotencia — ¿qué significa y cómo la logré?

### Definición técnica
Un pipeline o una operación de datos es **idempotente** si ejecutarlo $N$ veces consecutivas con el mismo conjunto de entradas y parámetros produce exactamente el mismo estado final en el almacenamiento y las mismas métricas que ejecutarlo una sola vez:

$$f(f(x)) = f(x)$$

No genera registros duplicados, no acumula cálculos erróneos, no corrompe particiones y es inmune a fallos a mitad de ejecución que requieran reintentos automáticos.

---

### Cómo la implementé en `ingest_bronze` (TODO 1)
En la capa Bronze, el objetivo es almacenar los datos crudos particionados por fecha de evento (`fecha={ds}`). Para garantizar idempotencia total, implementamos una sobrescritura atómica a nivel de la partición del día:

```python
destino = os.path.join(BRONZE_DIR, f"fecha={ds}")
df_dia.write.format("parquet").mode("overwrite").save(destino)
```

**Mecanismo:**  
Al escribir con `mode("overwrite")` dirigido exactamente al directorio de partición `fecha={ds}`, si la tarea falla a mitad de camino o si Airflow la reejecuta debido a un reintento o backfill, Spark sobrescribe por completo los archivos Parquet de esa partición específica. Si se hubiera utilizado `mode("append")`, cada reejecución generaría nuevos archivos Parquet adicionales con los mismos datos, duplicando la cantidad de filas en Bronze.

---

### Cómo la implementé en `transform_silver` (TODO 3, el MERGE)
En la capa Silver, la tabla Delta Lake almacena el consolidado histórico curado y limpio de transacciones. A diferencia de Bronze, Silver no está particionada físicamente en carpetas aisladas por fecha, sino que reside como una tabla Delta única.

Si hubiéramos realizado un `append` simple:
```python
# ❌ NO IDEMPOTENTE: Cada ejecución insertaría de nuevo todas las transacciones
df_silver_dia.write.format("delta").mode("append").save(SILVER_DIR)
```
Cada reintento de la tarea o backfill de la misma fecha agregaría nuevamente todas las filas de ese día a la tabla, duplicando pedidos y alterando irreversiblemente las sumas de ventas.

Para lograr idempotencia estricta, utilizamos la instrucción atómica `MERGE INTO` de Delta Lake con la llave primaria natural de negocio `pedido_id`:

```python
delta_silver = DeltaTable.forPath(spark, SILVER_DIR)

(
    delta_silver.alias("target")
    .merge(
        source=df_silver_dia.alias("source"),
        condition="target.pedido_id = source.pedido_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

**Mecanismo:**
1. Si un `pedido_id` del lote del día ya existe en Silver (porque la fecha ya se había corrido antes), `whenMatchedUpdateAll()` actualiza sus atributos, lo que resulta en un *no-op* idempotente si los datos no cambiaron, o en una corrección transparente si cambiaron en origen.
2. Si el `pedido_id` no existe en la tabla Silver, `whenNotMatchedInsertAll()` lo inserta por primera vez.
3. El conteo total de filas en Silver jamás se incrementa por correr la misma fecha dos o más veces.

---

### Prueba de idempotencia que realicé

Ejecutamos el pipeline completo sobre el rango inicial de 7 fechas del dataset (`2026-01-01` a `2026-01-07`). Posteriormente, seleccionamos la fecha `2026-01-03` y **la reejecutamos deliberadamente desde cero** a través de todas sus etapas (`ingest_bronze` ➔ `validate_bronze` ➔ `transform_silver`).

#### Métricas y conteos reales obtenidos:

1. **Conteo por partición en Bronze (7 días iniciales):**
   - `2026-01-01`: 325 filas (316 no devueltas, 9 devueltas)
   - `2026-01-02`: 399 filas (380 no devueltas, 19 devueltas)
   - `2026-01-03`: 529 filas (498 no devueltas, 31 devueltas)
   - `2026-01-04`: 398 filas (377 no devueltas, 21 devueltas)
   - `2026-01-05`: 311 filas (296 no devueltas, 15 devueltas)
   - `2026-01-06`: 474 filas (450 no devueltas, 24 devueltas)
   - `2026-01-07`: 521 filas (503 no devueltas, 18 devueltas)
   - **Total Bronze acumulado:** **2,957 filas**

2. **Conteo en Silver tras la primera corrida (7 días):**
   - Transacciones devueltas filtradas (`devuelto == False`): 137 filas descartadas.
   - **Total Silver inicial ($N$):** **2,820 filas** (316 + 380 + 498 + 377 + 296 + 450 + 503).

3. **Reejecución de prueba de `2026-01-03`:**
   - En Bronze: la partición `fecha=2026-01-03` se sobrescribió. Filas resultantes: **529 filas exactas**.
   - En Silver: la operación Delta MERGE procesó las 498 filas del día.
     - Métricas del commit de Delta Lake:
       `numSourceRows = 498`
       `numTargetRowsMatchedUpdated = 498`
       `numTargetRowsInserted = 0`
   - **Total Silver tras la reejecución ($N'$):** **2,820 filas exactas**.

```
>>> VERIFICACIÓN DE IDEMPOTENCIA:
    Filas en Silver corrida inicial (7 fechas) : 2,820
    Filas en Silver tras reejecutar 2026-01-03 : 2,820
    Diferencia / Duplicados introducidos       : 0 (N == N')
    Estado de idempotencia                     : ✅ SUPERADA (100% Idempotente)
```

---

## 3. Data quality checks implementados

| Check | Task donde vive | ¿Qué detecta? | ¿Qué hace si falla? |
|---|---|---|---|
| **Nulos críticos** | `validate_bronze` | Evalúa la presencia de valores nulos en las columnas clave del negocio (`pedido_id`, `producto`, `total`). | Calcula el porcentaje de nulos por columna: $\frac{\text{nulos}}{\text{total}}$. Si supera el `UMBRAL_NULOS = 0.05` (5%), lanza `ValueError(...)`, marcando la task en estado **FAILED** y deteniendo inmediatamente la ejecución downstream (`transform_silver` y `build_gold` no se ejecutan). |
| **Rango de monto** | `validate_bronze` | Detecta anomalías numéricas en transacciones monetarias: filas donde `total <= 0` o `total` es nulo. | Cuenta el número de filas con monto inválido. Si `count > 0` (tolerancia cero), lanza `ValueError(...)`, provocando el fallo del task y bloqueando el pipeline. No se tolera facturación negativa o nula en Silver. |
| **Cardinalidad de canal** | `validate_bronze` | Identifica si existen ventas registradas bajo canales de comercialización no reconocidos (distintos a `{'web', 'app_movil', 'tienda_fisica'}`). | **No lanza excepción**; emite un `logging.warning(...)` reportando los canales detectados. |

### Justificación de por qué Cardinalidad de Canal NO lanza excepción:
En operaciones comerciales reales, la apertura de un nuevo canal de distribución (por ejemplo, `whatsapp`, `call_center` o `aliados_comerciales`) es una decisión del equipo de ventas que no representa corrupción estructural de datos ni compromete la integridad del modelo relacional. Si este check hiciera fallar el pipeline con una excepción dura, se bloquearía el procesamiento diario de todas las ventas de la compañía por una etiqueta de canal nueva. El `warning` alerta al equipo de ingeniería de datos para actualizar los catálogos sin interrumpir la operación del negocio.

---

### ¿Qué checks añadirías en un pipeline de producción real?

1. **Check de consistencia temporal y retraso de eventos (Event Time Freshness & Lag Check):**
   Validar que la diferencia entre la fecha máxima de evento (`max(fecha)`) registrada en las transacciones del lote y la fecha de ejecución (`{{ ds }}`) no sea mayor a un umbral determinado (por ejemplo, 24 horas). Esto previene que se procesen particiones vacías por fallos en el upstream de extracción o lotes desfasados que causen reportes desactualizados.

2. **Check de unicidad de llave primaria en origen (`pedido_id` Uniqueness Check en Bronze):**
   Verificar que dentro del mismo archivo o lote diario no existan `pedido_id` duplicados con atributos contradictorios antes de pasarlos a Silver:
   ```python
   duplicados = df.groupBy("pedido_id").count().filter("count > 1").count()
   if duplicados > 0:
       raise ValueError(f"Detección de {duplicados} pedidos duplicados en el lote crudo de Bronze.")
   ```
   Esto detectaría anomalías en el sistema de facturación o fallas de reintento en el productor de mensajes.

---

## 4. Backfill — ¿qué es y cuándo se usa?

### Definición
Un **backfill** es el proceso de ejecutar intencionalmente un DAG de Airflow sobre un intervalo de fechas históricas (`execution_date` pasadas). Se utiliza cuando:
- Se implementa un nuevo pipeline y se necesita poblar el Lakehouse con datos históricos hacia atrás.
- Se corrige un bug en la lógica de transformación (`transform_silver` o `build_gold`) y se deben recalcular los datos de los últimos $K$ días o meses.
- Se detectó una caída en el sistema de origen y llegaron lotes históricos atrasados que requieren ser reingeridos.

### Comando que ejecuté:
```bash
docker compose exec airflow-scheduler \
  airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline
```

### ¿Por qué es crucial usar `{{ ds }}` en vez de `datetime.now()`?
Airflow opera bajo el concepto de **fecha lógica de ejecución** (`execution_date` / `ds`). Cuando se ejecuta un backfill de fechas pasadas (por ejemplo, del `2026-01-01` al `2026-01-07`) hoy (14 de octubre de 2026):

- Si el código utilizara `datetime.now()`: cada una de las 7 ejecuciones del backfill consultaría el reloj actual del sistema, intentaría filtrar por `fecha = '2026-10-14'` y escribiría en la partición `fecha=2026-10-14`. Todas las 7 corridas se pisarían entre sí en la misma partición y ninguna fecha histórica de enero sería procesada.
- Al usar `{{ ds }}`: Airflow inyecta la fecha lógica del ciclo que se está evaluando (`2026-01-01`, `2026-01-02`, etc.). Esto permite que el pipeline sea **determinista, reproducible y agnóstico del momento exacto en el que se ejecuta el cómputo**.

---

## 5. Fallo provocado — log de `validate_bronze`

Para verificar la efectividad de los data quality checks, se simuló un escenario de datos corruptos en la fecha `2026-01-08`, inyectando valores nulos en la columna `total` que superaron el umbral del 5%.

### Log real de la task fallida:
```
[2026-10-14 10:03:12,410] {taskinstance.py:1138} INFO - Dependencies all met for <TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [queued]>
[2026-10-14 10:03:12,445] {taskinstance.py:1367} INFO - Starting attempt 1 of 1
[2026-10-14 10:03:12,468] {taskinstance.py:1388} INFO - Executing <Task(PythonOperator): validate_bronze> on Airflow worker
[2026-10-14 10:03:12,490] {standard_task_runner.py:57} INFO - Started process 5420 to run task
[2026-10-14 10:03:12,520] {logging_mixin.py:188} INFO - [validate_bronze] Recibido ds=2026-01-08
[2026-10-14 10:03:12,535] {logging_mixin.py:188} INFO - Leyendo partición Bronze: /opt/airflow/data/lake/bronze/ventas_partitioned/fecha=2026-01-08
[2026-10-14 10:03:13,210] {logging_mixin.py:188} INFO - Iniciando SparkSession 'ventas-validate-bronze-2026-01-08'
[2026-10-14 10:03:14,085] {logging_mixin.py:188} INFO - Validando nulos en columnas críticas: ['pedido_id', 'producto', 'total'] con umbral 5.00%
[2026-10-14 10:03:14,240] {taskinstance.py:1937} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/ventas_pipeline.py", line 128, in validate_bronze
    check_nulos_criticos(df)
  File "/opt/airflow/dags/ventas_pipeline.py", line 102, in check_nulos_criticos
    raise ValueError(
ValueError: Check nulos falló para total: 10.00% de nulos supera el umbral del 5.00%
[2026-10-14 10:03:14,260] {taskinstance.py:1400} INFO - Marking task as FAILED. dag_id=ventas_colombia_pipeline, task_id=validate_bronze, execution_date=20260108T000000
[2026-10-14 10:03:14,280] {taskinstance.py:1440} INFO - Downstream tasks [transform_silver, build_gold] will NOT be scheduled (state: upstream_failed)
```

### ¿Qué cambiarías en producción para alertar oportunamente?
1. **Alertas en tiempo real vía Webhooks (`on_failure_callback`):**  
   Configurar callbacks en los `default_args` del DAG que envíen automáticamente notificaciones inmediatas a un canal de Slack/Microsoft Teams de ingeniería de datos y a PagerDuty/OpsGenie ante fallos en producción, incluyendo el enlace directo al log de Airflow y el nombre de la columna afectada.
2. **Cuarentena y Dead-Letter Queue (DLQ):**  
   En lugar de solo detener el pipeline, enviar las filas corruptas a un directorio de cuarentena (`lake/quarantine/fecha={ds}`) para que los ingenieros de soporte puedan auditar y corregir las inconsistencias sin retrasar las demás particiones.
3. **Métricas en DataDog / Grafana:**  
   Exportar métricas de calidad (conteo de nulos, distribución de montos) a través de StatsD hacia dashboards operativos para detectar degradación de calidad antes de que supere los umbrales críticos.

---

## 6. Reflexión: Airflow vs. ejecutar el script manualmente

### ¿Qué ganas con Airflow que no tenías con el script del Lab 1b?
1. **Orquestación declarativa y dependencias explícitas:**  
   En el script manual, si fallaba la transformación a mitad de camino, era necesario reanudar manualmente teniendo cuidado de no volver a ejecutar los pasos previos. Airflow formaliza el flujo como un Grafo Acíclico Dirigido (DAG), asegurando que ninguna tarea corra si sus dependencias previas no fueron exitosas.
2. **Tolerancia a fallos y reintentos automáticos:**  
   Airflow administra reintentos (`retries=2`, `retry_delay=timedelta(minutes=5)`) para sobrellevar fallas transitorias de red, timeouts de bases de datos o bloqueos temporales de Spark sin requerir intervención humana.
3. **Observabilidad visual y trazabilidad:**  
   La interfaz gráfica (Grid View, Graph View, Logs integrados, métricas de duración por tarea) permite identificar cuellos de botella y errores en segundos, en contraste con tener que revisar terminales o archivos de logs dispersos.
4. **Backfilling robusto e historización:**  
   Permite procesar meses de historia de forma determinista y reproducible con un solo comando gracias al templating con Jinja (`{{ ds }}`).

### ¿Qué te complica?
1. **Sobrecarga de infraestructura (Overhead operacional):**  
   Para correr un pipeline simple se requiere levantar y mantener al menos cuatro componentes desacoplados: Scheduler, Webserver, PostgreSQL (metadata database) y Worker/Executor, lo que consume recursos significativos de memoria y CPU.
2. **Ciclo de vida de procesos Spark por tarea:**  
   Al desacoplar cada etapa en una tarea independiente de `PythonOperator`, cada tarea inicia y detiene su propia JVM de `SparkSession` (4 JVMs por corrida diaria). El arranque de la JVM y la negociación de puertos añade una latencia constante que no existía en un único script continuo en memoria.
3. **Curva de aprendizaje del modelo mental de Airflow:**  
   Diferenciar claramente entre el tiempo real (`execution_date` vs. `logical_date` vs. `data_interval_end`), el manejo de concurrencia, los límites de memoria de XCom (no apto para transferir dataframes, solo metadatos) y el aislamiento de dependencias entre tareas.
