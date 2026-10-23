# dag_design.md — Lab 4a: Diseño del DAG

**Curso:** ST1630 · **Semana:** S11 · **Fecha:** 2026-10-17
**Estudiante:**
* Athina Alejandra Cappelleti García (aacappellg@eafit.edu.co)
* David Alejandro Gutiérrez Leal (dagutierrl@eafit.edu.co)
* Emmanuel Álvarez Castrillón (ealvarezc1@eafit.edu.co)
* Ginna Alejandra Valencia Macuace (gavalencim@eafit.edu.co)
* Mariamny Del Valle Ramírez Telles (mvramirezt@eafit.edu.co)

> Copia este archivo a `entregas/<tu-usuario>/dag_design.md` y complétalo
> con evidencia de TU propia ejecución (conteos reales, logs reales) —
> no respuestas genéricas sobre cómo funciona Airflow en general.

## Justificación del schedule

**schedule_interval elegido:** `@daily`

**Justificación:** Los datos de ventas llegan por día (el dataset tiene una
columna `fecha` y `ingest_bronze` ingesta una partición por día), así que
una corrida diaria encaja con la granularidad de la fuente: correr más seguido
no traería datos nuevos y correr menos seguido atrasaría las tablas. Además,
un equipo de negocio como el de Nequi normalmente revisa los indicadores de
ventas por región y el top de productos (las dos tablas de Gold) al inicio
del día, sobre la foto del día anterior. Con `@daily` la corrida corre a
medianoche UTC y las tablas de Gold ya están actualizadas cuando el equipo
las consulta.

**¿Por qué `catchup=False`?** Con `catchup=False`, al activar el DAG Airflow
solo lanza la corrida del último intervalo y sigue hacia adelante; no
reprocesa todo el historial desde `start_date` (2026-01-01). Si el DAG hubiera
estado 30 días sin activarse y lo activara con `catchup=True`, Airflow
crearía de golpe 30 corridas (una por día pendiente) y cada una levantaría
4 sesiones de Spark. En mi propia ejecución vi qué pasa cuando varias fechas
corren en paralelo: al lanzar el backfill de 7 fechas, 3 de ellas
(2026-01-01, 01-03 y 01-05) fallaron en `transform_silver` con
`ConcurrentAppendException`, porque varios MERGE intentaron escribir a la
misma tabla Delta de Silver al mismo tiempo. Con 30 corridas simultáneas el
problema sería peor, además de saturar la memoria del contenedor. Con
`catchup=False` el backfill de fechas pasadas es una decisión explícita
(comando manual) y no un efecto secundario de activar el DAG.

Comandos que ejecuté en esta parte:

```
# Backfill de las 7 fechas lanzadas en paralelo (3 fallaron en transform_silver)
docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline

# Causa del fallo, buscada en los logs de transform_silver de 2026-01-03
docker compose exec airflow-scheduler bash -c "grep -rhoE 'Concurrent[A-Za-z]*Exception|Mkdirs failed|Permission denied|AnalysisException[^.]*' '/opt/airflow/logs/dag_id=ventas_colombia_pipeline/run_id=backfill__2026-01-03T00:00:00+00:00/task_id=transform_silver/' | sort | uniq -c"
#   salida: 3 ConcurrentAppendException

# Reintento de las 3 fechas fallidas, una por una
foreach ($d in "2026-01-01","2026-01-03","2026-01-05") { docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s $d -e $d ventas_colombia_pipeline }
```

## Idempotencia — ¿qué significa y cómo la logré?

**Definición:** Un pipeline es idempotente si ejecutarlo N veces con la
misma entrada produce el mismo resultado que ejecutarlo 1 vez.

**Cómo la implementé en `ingest_bronze` (TODO 1):** Escribo la partición del
día con `df_dia.write.format("parquet").mode("overwrite").save(destino)`,
donde `destino` es `.../ventas_partitioned/fecha={ds}`. Como el `overwrite`
apunta solo a la carpeta de ese día, un reintento o un backfill repetido
reemplaza esa partición por el mismo contenido en lugar de agregarle filas, y
no toca las particiones de los demás días. Con `append` cada reejecución
duplicaría las filas del día.

**Cómo la implementé en `transform_silver` (TODO 3, el MERGE):** Si Silver ya
existe, hago un MERGE de Delta Lake con la condición
`target.pedido_id = source.pedido_id`, con `whenMatchedUpdateAll()` y
`whenNotMatchedInsertAll()`. Si el `pedido_id` ya está en Silver, la fila se
actualiza (con los mismos valores si los datos no cambiaron); si no está, se
inserta. Por eso, si el task falla justo después del MERGE y Airflow lo
reintenta, el segundo MERGE encuentra todos los `pedido_id` ya presentes y no
agrega filas. Un `append` simple habría insertado otra vez todas las filas de
la partición en cada reintento o backfill repetido, y Silver tendría
`pedido_id` duplicados. Un límite que descubrí en la práctica: el MERGE
protege contra reejecuciones, pero no contra escrituras simultáneas a la
misma tabla.

**Prueba de idempotencia que realicé:** Primero corrí el backfill de las 7
fechas (2026-01-01 a 2026-01-07) y medí el estado de las capas con un script
de conteo. Luego repetí las 7 fechas una por una (para evitar conflictos de
concurrencia), con `--reset-dagruns` para obligar a Airflow a reejecutar
corridas ya exitosas, y volví a contar. Los conteos antes y después fueron
idénticos.

Comandos que ejecuté en esta prueba:

```
# Conteo de filas de Bronze y Silver (script contar.py, solo para medir; no se entrega)
docker compose exec airflow-scheduler python /opt/airflow/dags/contar.py 2>&1 | Select-String "BRONZE|SILVER"

# Repetir las 7 fechas, una por una (el bucle lo interrumpí después de 2026-01-06)
foreach ($d in "2026-01-01","2026-01-02","2026-01-03","2026-01-04","2026-01-05","2026-01-06","2026-01-07") { docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s $d -e $d ventas_colombia_pipeline }

# Repetir por separado la fecha que quedó pendiente
docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s 2026-01-07 -e 2026-01-07 ventas_colombia_pipeline

# Verificar que las 7 corridas están en success y cuándo se ejecutaron
docker compose exec airflow-scheduler airflow dags list-runs -d ventas_colombia_pipeline
```

Contenido de `contar.py` (copiado dentro de `dags/` solo para medir):

```python
import sys
sys.path.insert(0, "/opt/airflow/dags")
from ventas_pipeline import _spark_session, BRONZE_DIR, SILVER_DIR

spark = _spark_session("contar")
spark.sparkContext.setLogLevel("ERROR")
try:
    b = spark.read.parquet(BRONZE_DIR)
    print("BRONZE filas:", b.count())
except Exception as e:
    print("BRONZE no disponible:", e)
try:
    s = spark.read.format("delta").load(SILVER_DIR)
    print("SILVER filas:", s.count(), "| pedido_id distintos:", s.select("pedido_id").distinct().count())
except Exception as e:
    print("SILVER no disponible:", e)
spark.stop()
```

Salidas del conteo:

```
ANTES de repetir el backfill:
BRONZE filas: 2957
SILVER filas: 2820 | pedido_id distintos: 2820

DESPUÉS de repetir las 6 primeras fechas (01-01 a 01-06):
BRONZE filas: 2957
SILVER filas: 2820 | pedido_id distintos: 2820

DESPUÉS de repetir también 2026-01-07 (las 7 fechas):
BRONZE filas: 2957
SILVER filas: 2820 | pedido_id distintos: 2820
```

Bronze no creció (el `overwrite` por partición reemplaza el contenido del día)
y Silver mantiene 2820 filas, igual a sus `pedido_id` distintos, o sea que no
hay duplicados. Silver tiene menos filas que Bronze porque excluye los pedidos
devueltos.

## Data quality checks implementados

| Check | Task donde vive | ¿Qué detecta? | ¿Qué hace si falla? |
|---|---|---|---|
| Nulos críticos | `validate_bronze` | Calcula la fracción de nulos de cada columna de `COLUMNAS_CRITICAS` (`pedido_id`, `producto`, `total`); si alguna supera el 5% (`UMBRAL_NULOS`), hay datos corruptos o incompletos. | Lanza `ValueError` con la columna y el porcentaje; la task queda en `failed` y `transform_silver`/`build_gold` quedan en `upstream_failed` (no corren). |
| Rango de monto | `validate_bronze` | Cuenta las filas con `total <= 0`; una venta con monto negativo o cero no tiene sentido de negocio. | Si hay al menos una fila, lanza `ValueError` con el conteo exacto; el pipeline se detiene igual que con el check anterior. |
| Cardinalidad de canal | `validate_bronze` | Cuenta los valores distintos de `canal`; si hay más de `CANALES_ESPERADOS_MAX` (4), algo cambió en la fuente. | Solo registra un `logging.warning`, no lanza excepción. Tiene sentido porque un canal nuevo puede ser un cambio legítimo del negocio (por ejemplo, un canal de venta recién lanzado), y no debería tumbar el pipeline; en cambio, nulos masivos o montos no positivos son datos rotos con certeza. |

**¿Qué check añadirías en un pipeline de producción real?**

1. **Duplicados de la clave de negocio:** verificar que no haya `pedido_id`
   repetidos dentro de la partición de entrada. Un `pedido_id` duplicado en
   el mismo lote haría que el MERGE tenga que elegir entre dos filas
   (Delta falla si varias filas de origen coinciden con la misma fila
   destino).
2. **Volumen esperado (completitud):** comparar el número de filas del día
   contra el de días anteriores (por ejemplo, alertar si hay menos del 50% o
   más del 200% de lo habitual). En mi ejecución, `ingest_bronze` ya trata
   "0 filas" como caso normal, así que una caída silenciosa del volumen no
   haría fallar nada.
3. **Consistencia de la fila:** verificar que `total = cantidad * precio_unit`
   (en las filas de muestra del CSV se cumple, por ejemplo 5 × 228500 =
   1142500) y que `fecha` coincida con la partición `ds` que se está
   ingiriendo.

## Backfill — ¿qué es y cuándo se usa?

**Definición:** Un backfill es ejecutar un DAG para un rango de fechas pasadas
(intervalos de datos que ya "ocurrieron"), para procesar datos históricos que
no se procesaron, o reprocesar fechas después de corregir un error en el
código o en los datos de origen. Cada fecha del rango produce su propia
corrida del DAG, con su propio `ds`.

**Comando que ejecuté:**

```
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-01 -e 2026-01-07 ventas_colombia_pipeline
```

Para repetir fechas ya exitosas (la prueba de idempotencia) y reprocesar
2026-01-08 después del fallo provocado, usé una fecha por vez con
`--reset-dagruns -y`:

```
docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s 2026-01-01 -e 2026-01-01 ventas_colombia_pipeline
```

En PowerShell, con un `foreach` sobre las 7 fechas:

```
foreach ($d in "2026-01-01","2026-01-02","2026-01-03","2026-01-04","2026-01-05","2026-01-06","2026-01-07") { docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s $d -e $d ventas_colombia_pipeline }
```

**¿Por qué es importante que `ingest_bronze` use `{{ ds }}` en vez de,
por ejemplo, "la fecha de hoy" (`datetime.now()`)?** `ds` es la fecha lógica
de la corrida, y Airflow la entrega a la función como parámetro. En un
backfill de fechas pasadas, cada corrida tiene un `ds` distinto (2026-01-01,
2026-01-02...), y `ingest_bronze` usa ese valor para filtrar el CSV y para
escribir su partición `fecha={ds}`. Si usara `datetime.now()`, todas las
corridas del backfill filtrarían la fecha real del sistema (octubre de
2026), que no tiene filas en el dataset: no se ingestaría ningún dato
histórico, o todas las corridas escribirían sobre la misma partición. Además
el resultado dependería de cuándo se ejecuta, así que reintentar mañana
daría un resultado distinto al de hoy, y eso rompe la idempotencia y la
reproducibilidad. Lo comprobé en la práctica: la corrida programada de
`2026-10-16` sale sin datos para esa fecha, mientras que el backfill de enero
sí encontró filas, porque cada corrida filtra por su propio `ds`.

## Fallo provocado — log de `validate_bronze`

Pega aquí el log completo de la task cuando falló (Parte 4 del README):

Para provocar el fallo vacié la columna `total` en 41 de las 510 filas de
2026-01-08 (8.04%, por encima del umbral de 5%). `validate_bronze` falló en
sus 3 intentos (el DAG tiene 2 reintentos con 5 minutos de espera, por lo que
la corrida tardó unos 10 minutos y medio). Comandos que ejecuté en esta parte:

```
# Ver el encabezado del CSV y contar las filas de 2026-01-08
Get-Content datos\ventas_colombia_lab4a.csv -TotalCount 3
(Select-String -Path datos\ventas_colombia_lab4a.csv -Pattern "2026-01-08").Count
#   salida: 510

# Vaciar la columna total en el 8% de las filas de 2026-01-08
python -c "
import csv, math
path = 'datos/ventas_colombia_lab4a.csv'
with open(path, newline='', encoding='utf-8') as f:
    rows = list(csv.reader(f))
h = rows[0]
i_f, i_t = h.index('fecha'), h.index('total')
idx = [i for i, r in enumerate(rows[1:], 1) if r[i_f] == '2026-01-08']
n = math.ceil(len(idx) * 0.08)
for i in idx[:n]:
    rows[i][i_t] = ''
with open(path, 'w', newline='', encoding='utf-8') as f:
    csv.writer(f).writerows(rows)
print('filas 2026-01-08:', len(idx), '| vaciadas:', n)
"
#   salida: filas 2026-01-08: 510 | vaciadas: 41

# Correr solo esa fecha (validate_bronze debe fallar)
docker compose exec airflow-scheduler airflow dags backfill -s 2026-01-08 -e 2026-01-08 ventas_colombia_pipeline

# Extraer del log del último intento el error del check
docker compose exec airflow-scheduler bash -c "grep -E 'ValueError|Traceback|Check' '/opt/airflow/logs/dag_id=ventas_colombia_pipeline/run_id=backfill__2026-01-08T00:00:00+00:00/task_id=validate_bronze/attempt=3.log'"

# Medir el efecto en Bronze y Silver
docker compose exec airflow-scheduler python /opt/airflow/dags/contar.py 2>&1 | Select-String "BRONZE|SILVER"

# Dejar todo limpio: regenerar el CSV y reprocesar la fecha
python datos/gen_dataset.py
docker compose exec airflow-scheduler airflow dags backfill --reset-dagruns -y -s 2026-01-08 -e 2026-01-08 ventas_colombia_pipeline
docker compose exec airflow-scheduler python /opt/airflow/dags/contar.py 2>&1 | Select-String "BRONZE|SILVER"
```

Líneas relevantes del log del
último intento (`attempt=3`):

```
Traceback (most recent call last):
    raise ValueError(
ValueError: Check nulos críticos FALLÓ: la columna 'total' tiene 8.04% de nulos (41 de 510 filas), supera el umbral de 5%.
[2026-10-17T21:42:12.606+0000] {standard_task_runner.py:110} ERROR - Failed to execute job 138 for task validate_bronze (Check nulos críticos FALLÓ: la columna 'total' tiene 8.04% de nulos (41 de 510 filas), supera el umbral de 5%.; 60569)
```

Resumen de la corrida en la consola del backfill:

```
Task instance <TaskInstance: ventas_colombia_pipeline.validate_bronze backfill__2026-01-08T00:00:00+00:00 [failed]> failed
Task instance <TaskInstance: ventas_colombia_pipeline.transform_silver backfill__2026-01-08T00:00:00+00:00 [upstream_failed]> with state upstream_failed
Task instance <TaskInstance: ventas_colombia_pipeline.build_gold backfill__2026-01-08T00:00:00+00:00 [upstream_failed]> with state upstream_failed
Marking run <DagRun ventas_colombia_pipeline @ 2026-01-08 ...> failed
```

Efecto sobre los datos, medido con el script de conteo:

```
Antes del fallo:                BRONZE 2957 | SILVER 2820 (2820 pedido_id distintos)
Después del fallo provocado:    BRONZE 3467 | SILVER 2820 (2820 pedido_id distintos)
Tras regenerar el CSV y
reprocesar 2026-01-08:          BRONZE 3467 | SILVER 3310 (3310 pedido_id distintos)
```

Bronze subió en 510 filas porque `ingest_bronze` ya había escrito la partición
corrupta de esa fecha antes de que `validate_bronze` la rechazara (Bronze
guarda el dato tal como llega), pero Silver no cambió: la validación evitó
que los nulos pasaran a Silver y Gold. Al regenerar el CSV con
`python datos/gen_dataset.py` y reprocesar esa fecha, el `overwrite` de la
partición reemplazó los datos corruptos (Bronze se quedó en 3467) y Silver
creció a 3310 con el día nuevo, otra vez sin duplicados.

**¿Qué cambiarías en el pipeline para que este tipo de fallo no llegue
a producción sin que nadie se entere?** En esta ejecución el fallo solo es
visible si alguien abre la UI de Airflow, y eso no basta en producción. Haría
tres cambios:

1. **Alertas activas:** configurar `on_failure_callback` (o
   `email_on_failure`) para que el fallo notifique al equipo por Slack o
   correo con el nombre del check, la columna y el porcentaje, que ya vienen
   en el mensaje del `ValueError`. Solo el último intento debería alertar,
   para no mandar un aviso por cada reintento.
2. **Cuarentena de la partición corrupta:** que `ingest_bronze` o
   `validate_bronze` mueva los datos rechazados a una ubicación separada
   (por ejemplo `bronze_quarantine/fecha=...`), para que el rastro de datos
   malos no se mezcle con Bronze y se pueda inspeccionar o reprocesar.
3. **Visibilidad y plazos:** un dashboard o panel con el estado de los
   checks por día, y un SLA (`sla=`) en las tasks para avisar si la tabla de
   Gold no se actualizó a la hora en que el equipo de negocio la espera.

## Reflexión: Airflow vs. ejecutar el script manualmente

**¿Qué ganas con Airflow que no tenías con el script de Lab 1b?**
Ejecución automática y programada, sin depender de que alguien lo corra a
mano. Reintentos automáticos (vi cómo `ingest_bronze` y `transform_silver`
se reintentaron solos). Dependencias explícitas entre tasks: cuando
`validate_bronze` falló, `transform_silver` y `build_gold` no se
ejecutaron, y eso es justo lo que se necesita para que los datos corruptos no
avancen. Visibilidad: el Grid y el Graph muestran de un vistazo qué corrida y
qué task falló, y cada intento guarda su propio log, que usé para diagnosticar
los errores. Y el backfill: reprocesar un rango de fechas es un comando, con
una corrida independiente por fecha.

**¿Qué te complica?** El montaje inicial dio más trabajo que el pipeline:
tuve problemas de DNS de Docker al descargar la imagen, el CSV no estaba donde
el DAG lo buscaba (`PATH_NOT_FOUND`), hubo un error al escribir Bronze
(`Mkdirs failed`) por permisos en las carpetas de salida, y `pytest` no venía
instalado en el contenedor. Cada task levanta su propia JVM de Spark, así que
una sola fecha tarda cerca de un minuto y el backfill completo, más de diez.
Los reintentos con 5 minutos de espera alargaron la prueba del fallo a casi 11
minutos. Y la concurrencia: el backfill paralelo produjo
`ConcurrentAppendException` en Silver, de modo que la idempotencia del MERGE no
es suficiente si varias corridas escriben a la vez; tuve que repetir las fechas
fallidas una por una (una alternativa sería limitar `max_active_runs=1`).
Depurar implica leer logs largos de Spark entre mucho ruido, y eso es más
lento que correr un script local y ver el error en la consola.

Comandos que usé para diagnosticar esos problemas:

```
# Ver qué task falló en una corrida
docker compose exec airflow-scheduler airflow tasks states-for-dag-run ventas_colombia_pipeline "backfill__2026-01-01T00:00:00+00:00"

# Leer el final del log de una task (aquí ingest_bronze, intento 1)
docker compose exec airflow-scheduler bash -c "tail -n 60 '/opt/airflow/logs/dag_id=ventas_colombia_pipeline/run_id=backfill__2026-01-01T00:00:00+00:00/task_id=ingest_bronze/attempt=1.log'"

# Comprobar que el CSV estaba dentro del contenedor
docker compose exec airflow-scheduler ls -la /opt/airflow/data/raw

# Corregir permisos de la carpeta de salida
docker compose exec -u root airflow-scheduler bash -c "chmod -R a+rwx /opt/airflow/data/output"
```