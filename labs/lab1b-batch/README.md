# Lab 1b · Batch: S3, Glue, Athena, EMR, Spark

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Última edición:** 2026-08-13
**Peso:** parte del 30% de laboratorios (ver `../../docs/evaluacion.md`)
**Tiempo estimado:** 8-10 horas, distribuidas en 3 días
**Entrega:** antes del inicio de S6

> **Este es el primer laboratorio 100% en casa del semestre.** Lo
> haces de forma autónoma, sin sesión de clase — esta guía debe
> bastarte por sí sola. Si te atoras, revisa primero la sección de
> Troubleshooting antes de escribir al profesor.

## Objetivo

Construir un pipeline batch completo Bronze → Silver → Gold sobre un
dataset de ventas colombiano intencionalmente sucio, usando Delta Lake
para MERGE incremental ACID y time travel, y cerrar con un benchmark
real en Athena que conecta el resultado con lo visto en S4 (Parquet
vs. CSV) y S5 (shuffle físico, Catalyst Optimizer).

## Prerequisito y verificación

Necesitas el **Lab 1a completado**: bucket S3 con estructura
Bronze/Silver/Gold, rol IAM de mínimo privilegio, y (para esta sesión)
un clúster EMR en estado `WAITING`. Si tu clúster del Lab 1a ya no
existe, créalo de nuevo con `../lab1a-cloud-setup/scripts/create_emr.sh`
antes de seguir.

Verifica que todo esté listo:

```bash
aws emr list-clusters --active
```

**Output esperado:** al menos un clúster con `"Status": {"State": "WAITING"}`
y el nombre que le pusiste en el Lab 1a (`st1630-{tu-usuario}-emr`).

```bash
aws s3 ls s3://st1630-{tu-usuario}-{año}/ --recursive | head -20
```

**Output esperado:** los objetos `bronze/`, `silver/`, `gold/` y los
archivos de prueba del Lab 1a dentro de `bronze/ventas/`.

Si cualquiera de los dos comandos no muestra lo esperado, vuelve al
`README.md` del Lab 1a antes de continuar — este lab construye
directamente sobre esa infraestructura.

## El dataset — conoce tus datos antes de transformarlos

`datos/ventas_colombia_raw.csv` tiene **101.500 filas** y problemas de
calidad intencionales — duplicados, fechas en 5 formatos distintos,
35 variantes de texto para 6 regiones, valores de `total` que no
cuadran con `cantidad × precio_unit`, y más. Antes de escribir una sola
línea de limpieza, tu primer trabajo es **diagnosticar** esos
problemas ejecutando `scripts/00_profiling.py` y documentando lo que
encuentres en `data_profiling.md` (15% de la nota — ver Rúbrica).

Las 8 preguntas que debes responder (copia
`plantillas/data_profiling_template.md` a tu carpeta de entrega y
complétalo con el output real de tu ejecución):

1. ¿Cuántos duplicados exactos tiene el dataset?
2. ¿Cuántos formatos de fecha distintos puedes identificar? Lista al
   menos 3 con ejemplos reales del dataset.
3. ¿Cuántas variantes de "Bogotá" existen? Lístalas todas.
4. ¿Cuántas variantes de "app_movil" existen? Lístalas todas.
5. ¿Qué porcentaje de filas tiene `total <= 0` o nulo?
6. ¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente?
7. ¿Qué regla de negocio permite detectar errores en `total`?
8. Resume en 3-4 líneas tu plan de limpieza para Silver a partir de lo
   que encontraste.

No hay un ejemplo resuelto de este documento — la única forma de
responder estas preguntas es ejecutando el script tú mismo y leyendo lo
que tu propia terminal te muestra.

## Parte 1 — Profiling y exploración (Día 1, 2 horas)

1. Genera tu copia local del dataset (no viene en el repo — ver
   `datos/gen_dataset.py`, semilla fija, tu archivo va a ser
   byte-idéntico al de cualquier compañero):

   ```bash
   pip install pandas numpy   # si no las tienes
   python3 datos/gen_dataset.py
   ```

   Debe imprimir `Filas totales: 101,500` y un resumen de profiling —
   es solo un auto-chequeo, no lo copies como tu `data_profiling.md`.
2. Conéctate a tu clúster EMR (EMR Studio, recomendado, o SSH — ver
   Lab 1a Parte 5).
3. Sube el dataset crudo a S3:

   ```bash
   aws s3 cp datos/ventas_colombia_raw.csv s3://st1630-{tu-usuario}-{año}/raw/ventas_colombia_raw.csv
   ```

4. Copia `scripts/00_profiling.py` a tu Workspace de EMR Studio (o al
   master si trabajas por SSH), edita la variable `BUCKET`, y
   ejecútalo:

   ```bash
   spark-submit 00_profiling.py
   ```

5. Copia los outputs relevantes a `data_profiling.md` según vas
   respondiendo cada una de las 8 preguntas.

**Verifica:** el script debe mostrar la distribución completa de
`region` (35 filas distintas) y `canal` (20 filas distintas) — si ves
menos, revisa que estés leyendo el CSV completo y no una muestra.

**Error frecuente:** ver solo *algunas* de las 35 variantes de región
en el `.show()`. Por defecto `.show()` trunca a 20 filas — pasa un
número mayor: `.show(40, truncate=False)` (el script ya lo hace, pero
si armas tus propias queries de exploración, recuérdalo).

## Parte 2 — Bronze: ingesta sin transformar (Día 1, 1 hora)

Abre `scripts/01_bronze.py` (edita `BUCKET` primero) y completa los 4
bloques marcados con `# TODO`:

1. **TODO 1** — el schema explícito, con **todos los campos como
   `StringType`** — Bronze recibe el dato tal cual, sin asumir tipos;
   la conversión es responsabilidad de Silver (el script te explica
   por qué justo antes de este TODO — léelo antes de escribir código).
2. **TODO 2** — la lectura del CSV con ese schema.
3. **TODO 3** — las columnas de auditoría `_ingested_at` y
   `_source_file`.
4. **TODO 4** — la escritura a Delta en modo `append`.

Cada TODO trae su propia pregunta de clasificación (NARROW/WIDE) —
respóndela en el propio comentario del script antes de seguir.

Cuando termines, ejecútalo:

```bash
spark-submit 01_bronze.py
```

El script **ya trae resuelta** la verificación del final: confirma que
Bronze tiene TODOS los problemas del raw (nulos, duplicados, valores
corruptos deben seguir presentes — si Bronze saliera "limpio", algo se
transformó de más) y te guía para inspeccionar el primer commit de
`_delta_log`.

**Verifica:** el `assert` al final del script debe pasar sin error —
si falla, revisa tus TODOs; probablemente se te coló un `.filter()` o
un `.cast()` antes de la escritura.

**Error frecuente:** olvidar que `StringType` en TODOS los campos
significa que `cantidad`, `precio_unit` y `total` también son strings
en Bronze — si intentas hacer aritmética directamente sobre Bronze
(`df.select(F.sum("total"))`), vas a obtener un error o un resultado
sin sentido. Esa conversión es del script de Silver, no de este.

## Parte 3 — Silver: limpieza y normalización (Día 2, 3 horas)

Esta es la parte central del lab. `scripts/02_silver.py` tiene 6
bloques de TODO (3.2 a 3.7) en este orden exacto — ábrelo junto con
esta sección, son la misma numeración. Cada TODO trae, además del
código a completar, una pregunta de clasificación NARROW/WIDE que
respondes directamente en el comentario del script — esta guía te dice
**qué** hace cada paso y **dónde** buscar la respuesta, no te la
resuelve.

### 3.1 Deduplicación (dado, sin TODO)

`dropDuplicates()` sin argumentos, sobre las 14 columnas de Bronze. El
código ya está — tu trabajo es solo la clasificación NARROW/WIDE (el
comentario junto a la línea te da las preguntas guía). Ojo con la
intuición aquí: "quitar duplicados" se siente como una operación
simple, pero piensa en qué necesita Spark para saber si dos filas son
idénticas.

### 3.2 Fechas — el reto de los 5 formatos

Completa `FORMATOS_FECHA` con los patrones que identificaste en tu
propio profiling (Pregunta 2 de `data_profiling.md`), y construye la
columna `fecha_parsed` con `coalesce()` + `to_date()`. Piensa en qué
orden le conviene a la lista si dos formatos son ambiguos entre sí.

### 3.3 Normalización de región — el reto principal

El script te da el helper `construir_mapa()` ya resuelto (upper+trim+
cadena de `when()`) y un puñado de entradas de ejemplo en
`MAPA_REGION` (una por región). Tu trabajo es completar el resto del
diccionario con las variantes que **tú** encontraste en tu propio
profiling (Pregunta 3 de `data_profiling.md`, más lo que viste del
resto de regiones al correr `00_profiling.py`), hasta que la
verificación del script confirme que quedan exactamente 6 valores
distintos.

**Debes documentar tu decisión** en `pipeline_analysis.md`: ¿cómo
manejaste `'N/A'` y `'Desconocido'`?

> ⚠️ **Trampa intencional:** `upper()` **no le quita la tilde** a las
> palabras (`BOGOTA` sin tilde y `BOGOTÁ` con tilde son strings
> distintos para Spark). Si te sobran valores después de normalizar,
> el script te muestra cuáles con `.show()` — es la pista para
> encontrar el alias que te falta, no un error a ignorar.

### 3.4 Normalización de canal

Mismo patrón que 3.3 — completa `MAPA_CANAL` a partir de tu propio
profiling (Pregunta 4). Una diferencia de diseño intencional: el valor
canónico de salida es minúscula con guion bajo (`app_movil`, no
`APP_MOVIL`) — normalizar no siempre significa "todo mayúsculas";
depende de la convención que el resto del pipeline espera consumir.

### 3.5 Validación y recálculo de total

Regla de negocio: `total_correcto = cantidad × precio_unit`. Completa
los 3 pasos del TODO: castear `cantidad`/`precio_unit` a `double`,
filtrar solo los válidos (`> 0` ambos), y recalcular `total_silver`.
**No uses** el `total` del raw — ya viste en tu profiling por qué no es
confiable.

**Documenta en `pipeline_analysis.md` (Pregunta 2):** ¿por qué
recalcular en vez de filtrar directamente las filas con `total`
incorrecto? Cuantifica cuántas filas preserva cada estrategia.

### 3.6 Normalización de tipos

- `vendedor_id`: usa `regexp_extract()` para quedarte solo con la
  parte numérica, sin importar si venía como entero puro, `VEN-XXXX`,
  o el formato "mixto" que encontraste en el profiling. Si ya
  resolviste esto en el TODO de `00_profiling.py`, es la misma lógica.
- `email_cliente`: valida con `rlike()` contra un patrón de email, y
  agrega la columna booleana `email_valido`. **No elimines la fila** —
  un email roto no invalida el resto del pedido.

### 3.7 MERGE a Silver — ingesta incremental ACID

Este es el syntax nuevo de la semana (ver la Parte 3.7 del script para
la forma general del patrón `merge().whenMatchedUpdateAll()
.whenNotMatchedInsertAll().execute()`). Complétalo usando `pedido_id`
como clave de negocio.

El script ya trae resuelta la verificación con time travel: cuenta las
filas de la versión 0 de Silver (`option("versionAsOf", 0)`) contra la
versión actual, y muestra el historial completo de commits con
`DeltaTable.history()`.

### 3.8 Análisis del DAG

1. En EMR Studio: **Compute → tu clúster → Spark UI → pestaña SQL /
   DataFrame** (misma ruta que en el Lab 1a).
2. Abre el job de `02_silver.py` y cuenta los nodos **Exchange**.
3. Compara ese conteo contra las clasificaciones que anotaste en cada
   TODO — ¿coincide el número de Exchange que ves con el número de
   pasos que marcaste como WIDE?
4. Complementa esto con `df_silver.explain(mode="formatted")` (al
   final del script) — busca las líneas que empiezan con `Exchange` en
   el plan de texto; deben coincidir con lo que ves en el DAG visual.

Responde en `pipeline_analysis.md`, Pregunta 1.

## Parte 4 — Gold: KPIs de negocio (Día 2, 2 horas)

`scripts/03_gold.py` tiene 4 bloques de TODO (4.1, 4.2, 4.3, 4.4) — el
registro en Glue Catalog (4.5) viene un ejemplo resuelto para que
repitas el patrón en las otras 2 tablas.

### 4.1 KPI 1: Ventas por región y fecha

Completa el `groupBy("region", "fecha").agg(...)` con las 5 métricas
que pide el comentario del TODO: `ventas_totales`, `num_pedidos`,
`ticket_promedio`, `tasa_devolucion`, `calificacion_promedio`.

### 4.2 KPI 2: Top 3 productos por categoría

Dos pasos: un `groupBy("categoria","producto")` para las ventas por
producto, y una `Window.partitionBy("categoria").orderBy(...)` con
`rank() <= 3`. Presta atención a cuántos `Exchange` distintos genera
cada paso, y por qué el segundo no puede reusar el particionamiento
del primero aunque ambos agrupen por `categoria`.

### 4.3 KPI 3 (reto): Cohortes de clientes por canal

No hay implementación de referencia para este — lo diseñas tú desde
cero. Usa `canal`/`metodo_pago` (u otra combinación que te parezca más
interesante) para construir un KPI que responda una pregunta de
negocio real. Documenta tu diseño y tu clasificación NARROW/WIDE en
`pipeline_analysis.md`.

### 4.4 Escribir Gold con optimización

Completa el `OPTIMIZE ... ZORDER BY` sobre `ventas_region_fecha`. Elige
las columnas de ordenamiento pensando en qué va a filtrar la query de
Athena de la Parte 5.1 — ese es justo el filtro que el Z-order debería
volver más barato de escanear.

`OPTIMIZE` compacta archivos Parquet pequeños en archivos más grandes.
`ZORDER BY` va más allá: reordena físicamente las filas dentro de esos
archivos para que valores similares de las columnas elegidas queden
juntos — esto es lo que hace efectivo el predicado pushdown en Athena:
si Athena filtra por una de esas columnas, puede saltarse por completo
los archivos cuyo rango de Z-order no la incluye, en vez de leerlos y
descartar filas después.

### 4.5 Registrar en Glue Catalog

El script ya registra `gold_ventas_region_fecha` como ejemplo —
completa el mismo patrón (`CREATE TABLE IF NOT EXISTS ... USING DELTA
LOCATION '...'`) para las otras dos tablas Gold.

**Verifica:** `aws glue get-tables --database-name default --query 'TableList[].Name'`
debe listar las 3 tablas Gold.

## Parte 5 — Athena y benchmark (Día 3, 1.5 horas)

### 5.1 Query de negocio

`scripts/04_athena_benchmark.py` corre localmente (no en EMR — solo
necesita boto3 y tus credenciales de AWS Academy). Edita `BUCKET` y
`ATHENA_DATABASE`, y completa el TODO 5.1: el SQL (dialecto
Presto/Athena) que responda "las 5 regiones con más ventas totales en
los últimos 3 meses", contra la tabla `gold_ventas_region_fecha` que
registraste en Glue Catalog. Luego ejecútalo:

```bash
pip install boto3   # si no lo tienes
python3 scripts/04_athena_benchmark.py
```

El script mide y registra tiempo de ejecución y bytes escaneados
automáticamente.

### 5.2 Benchmark CSV vs. Parquet

Antes de correr el script necesitas una tabla de comparación: exporta
una muestra de 10.000 filas de tu Silver a CSV **sin particionar**, y
crea la tabla externa en Athena (el DDL de ejemplo está comentado
dentro de `04_athena_benchmark.py`, sección 5.2).

El script corre la misma query sobre ambas tablas y calcula el ratio
de bytes escaneados, guardando todo en `benchmark_resultados.md`
(el script lo genera automáticamente — no hace falta que lo escribas a
mano).

**Verifica:** `benchmark_resultados.md` debe existir en la raíz de tu
carpeta de entrega con una tabla de resultados y un ratio calculado.

## Parte 6 — Documentación (Día 3, 1 hora)

Completa `pipeline_analysis.md` (copia
`plantillas/pipeline_analysis_template.md`) con las 5 preguntas de
análisis:

1. ¿Cuántos Exchange tiene el pipeline completo (Bronze→Silver→Gold)?
   ¿A qué operación corresponde cada uno?
2. Recalcular vs. filtrar `total` — ¿cuántas filas preservaste con cada
   estrategia?
3. Robustez de la normalización de región ante una variante nueva.
4. Partición y shuffle files: filas por partición con 32 vs. 200.
5. Resultado real del benchmark Athena — ¿coincide con el ~9x teórico?

Respalda cada respuesta con evidencia concreta de tu propia ejecución
(números de tu Spark UI, tus propios conteos) — una respuesta que solo
repite la teoría de clase sin citar tu corrida real no cuenta como
completa (ver Rúbrica).

## Entregable — estructura del PR

```
labs/lab1b-batch/
└── entregas/
    └── <tu-usuario>/
        ├── data_profiling.md          # copiado de plantillas/ y completado
        ├── pipeline_analysis.md       # copiado de plantillas/ y completado
        ├── benchmark_resultados.md    # generado por 04_athena_benchmark.py
        └── bitacora_delegacion.md
```

No subas tus credenciales de AWS Academy ni ningún archivo `.pem`. El
dataset (`ventas_colombia_raw.csv`) tampoco se sube — ya está en el
repo dentro de `datos/`.

## Rúbrica

| Criterio | Peso | Completo | Parcial | Incompleto |
|---|---|---|---|---|
| **`data_profiling.md`** | 15% | Las 8 preguntas respondidas con output real copiado del script, incluyendo las 35/20 variantes contadas correctamente | Respuestas presentes pero con outputs incompletos o números que no coinciden con una ejecución real | Preguntas sin responder, o respuestas genéricas sin evidencia de haber ejecutado `00_profiling.py` |
| **Pipeline Silver** (limpieza + MERGE) | 30% | Las 8 transformaciones aplicadas correctamente, `region`/`canal` reducidos a 6/4 valores, MERGE funcionando con evidencia de time travel | Silver funciona pero con algún paso incompleto (p. ej. `vendedor_id` sin normalizar, o el MERGE sin verificación de versiones) | Silver no reduce a 6/4 valores de región/canal, o el MERGE no se ejecutó |
| **Pipeline Gold** (KPIs + Z-ordering) | 20% | Los 3 KPIs calculados correctamente, `OPTIMIZE ZORDER BY` aplicado y tablas registradas en Glue Catalog | KPIs correctos pero sin Z-ordering, o registro incompleto en Glue | KPIs ausentes o con lógica de agregación incorrecta |
| **Benchmark Athena documentado** | 15% | `benchmark_resultados.md` generado con tiempos y bytes escaneados reales de ambas queries, ratio calculado | Benchmark corrido pero incompleto (falta una de las dos queries) | Sin evidencia de haber ejecutado contra Athena real |
| **`pipeline_analysis.md`** (5 preguntas) | 20% | Las 5 preguntas respondidas citando evidencia concreta de la propia ejecución (números de Exchange, filas preservadas, bytes escaneados) | Respuestas presentes pero genéricas, repiten teoría sin conectar con la propia ejecución | Preguntas sin responder o con clasificaciones NARROW/WIDE incorrectas |

## Bitácora de delegación

Este lab sigue `../../docs/politica-ia.md`.

| Tarea | ¿Se puede delegar? | Nota |
|---|---|---|
| Sintaxis de PySpark (when/otherwise, regexp_extract, Window) | Sí | Dudas puntuales de sintaxis, bajo valor de aprendizaje memorizar |
| Troubleshooting de errores de Spark/Delta/Athena | Sí | Configuración de entorno, no decisión de diseño |
| Boilerplate ya dado en los scripts (imports, rutas, verificaciones) | N/A | No lo tocas — ya está resuelto; tu trabajo son los bloques `# TODO` |
| Completar el contenido de `MAPA_REGION`/`MAPA_CANAL` (3.3, 3.4) | **No** | Sale de TU propio profiling, no del de nadie más — es el ejercicio de criterio central del lab |
| Escribir el código de los TODO de `01_bronze.py`, `02_silver.py`, `03_gold.py` | Parcial | Puedes pedir ayuda de sintaxis puntual ("¿cómo se llama la función de X?"), pero la lógica y las decisiones (filtros, fórmulas, diseño del KPI 3) las escribes tú |
| Clasificar cada bloque como NARROW/WIDE y justificarlo | **No** | Es el objetivo de aprendizaje de la semana — un agente puede repetir la teoría, pero no puede confirmar qué viste en tu propio Spark UI |
| Diseñar el KPI 3 (cohortes) | **No** | No hay implementación de referencia — es el ejercicio de criterio de la Parte 4.3 |
| Escribir `pipeline_analysis.md` | **No** | Debe reflejar tu propia lectura de Spark UI y tus propios números — un agente no tiene acceso a tu ejecución real |
| Interpretar el resultado del benchmark Athena | **No** | Igual que arriba: es evidencia empírica de tu cuenta, no algo que un agente pueda inventar |

## Troubleshooting

| # | Error / síntoma | Causa probable | Solución |
|---|---|---|---|
| 1 | `Path does not exist` al leer `raw/ventas_colombia_raw.csv` | No subiste el CSV a S3, o la ruta/bucket no coincide | `aws s3 cp datos/ventas_colombia_raw.csv s3://<tu-bucket>/raw/ventas_colombia_raw.csv` y revisa que `BUCKET` en el script coincida exactamente |
| 2 | `region_silver` termina con más de 6 valores distintos | El `MAPA_REGION` no cubre alguna variante (frecuentemente: formas sin tilde como `BOGOTA`/`MEDELLIN`, que `upper()` no arregla) | Corre `df_region.select("region_silver").distinct().show()` y busca el valor sobrante; agrégalo a `MAPA_REGION` |
| 3 | `AnalysisException: Table or view not found` en `MERGE` | Es la primera ejecución y la tabla Silver todavía no existe | El script ya maneja esto con `DeltaTable.isDeltaTable(...)` — si el error persiste, revisa que `SILVER` apunte a una ruta donde tengas permisos de escritura (rol IAM del Lab 1a) |
| 4 | El MERGE falla con `AnalysisException` sobre columnas ambiguas | `whenMatchedUpdateAll()` sin alias claros cuando ambos DataFrames tienen columnas con el mismo nombre | Usa siempre `.alias("s")` / `.alias("n")` (ya están declarados en el `if` del TODO 3.7) y verifica que la condición del merge use esos alias, no los nombres de columna a secas |
| 5 | `spark-submit` falla con `ModuleNotFoundError: No module named 'delta'` | El paquete `delta-spark` no está instalado/configurado en el clúster | En EMR, agrega el paquete Delta al lanzar el job: `spark-submit --packages io.delta:delta-spark_2.12:3.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" 02_silver.py` |
| 6 | El benchmark Athena falla con `Query FAILED: Insufficient permissions` | El rol/usuario no tiene permisos sobre el bucket de resultados de Athena (`ATHENA_OUTPUT`) | Crea el prefijo `athena-results/` en tu bucket y confirma que tu rol IAM del Lab 1a incluye `s3:PutObject` sobre él (puede requerir ampliar la política si Athena escribe en una ruta nueva) |
| 7 | `get_query_execution` nunca pasa de estado `RUNNING` | Query costosa, o la tabla externa CSV mal definida (columnas no coinciden con el archivo) | Verifica el DDL de `CREATE EXTERNAL TABLE` contra las columnas reales del CSV exportado; revisa el estado manualmente desde la consola de Athena |
| 8 | `OPTIMIZE ... ZORDER BY` falla con `AnalysisException: ZORDER BY is only supported in Delta` | Intentaste correrlo sobre una tabla que no es Delta, o sin el catálogo Delta configurado | Confirma que `GOLD` apunta a una tabla escrita con `.format("delta")`, y que el `spark-submit` incluye los flags de Delta del punto 5 |
| 9 | El clúster EMR queda "Terminated" a mitad del Día 2 | Los clústers de EMR/Academy se auto-terminan tras inactividad o al expirar la sesión del Learner Lab (~4 horas) | Reinicia el clúster (Lab 1a, `create_emr.sh`), reatáchalo, y vuelve a correr desde el último script que no terminó — Bronze y Silver ya escritos no se pierden, viven en S3 |
| 10 | Costos inesperados / créditos de AWS Academy agotándose rápido | Clúster EMR olvidado encendido, o queries de Athena escaneando mucho más de lo esperado por falta de Z-ordering | Revisa `aws emr list-clusters --active` regularmente y apaga lo que no uses; en Athena, confirma que estás consultando las tablas Gold (con Z-order) y no escaneando Bronze/Silver completos sin filtrar |

## Referencias

- Documentación oficial de [Delta Lake](https://docs.delta.io/) —
  `MERGE INTO`, `OPTIMIZE`, `ZORDER BY`, time travel.
- [AWS Athena — Documentación oficial](https://docs.aws.amazon.com/athena/)
- [AWS Glue Catalog — Documentación oficial](https://docs.aws.amazon.com/glue/)
- Kleppmann, *Designing Data-Intensive Applications* — cap. 3
  (motores de almacenamiento) y cap. 10 (procesamiento batch).
- Slides de las clases S4 (formatos columnares, arquitectura medallion)
  y S5 (shuffle físico, Catalyst Optimizer, Delta Lake) del curso.
- `../lab1a-cloud-setup/README.md` — infraestructura base que este lab
  extiende.
