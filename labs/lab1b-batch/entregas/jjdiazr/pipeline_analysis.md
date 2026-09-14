# Análisis del pipeline — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 2026-08-22

**Estudiantes:**
- Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co
- Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
- Sebastián Durán Fernández — sduranf@eafit.edu.co
- Daniel Arcila Salazar — darcilas1@eafit.edu.co

> Ejecutado en el clúster EMR `j-08749873HH1YI3USSKPI` (emr-6.15.0,
> Spark 3.4.1, Delta Lake 2.4.0, 1 master + 1 core m5.xlarge).
> Toda la evidencia citada aquí sale de mis propias corridas —
> `profiling_output.txt`, `silver_output.txt` y `benchmark_resultados.md`.

## Pregunta 1 — Exchange del pipeline completo

¿Cuántos `Exchange` tiene el pipeline completo (Bronze → Silver →
Gold)? Identifica a qué operación corresponde cada uno y explica en
términos físicos (shuffle write, shuffle read) por qué esa operación es
WIDE.

→ El pipeline completo tiene **5 `Exchange`**: 0 en Bronze, 1 en Silver y
4 en Gold (ver tabla de evidencia abajo).

**Silver — `dropDuplicates()`.** Es WIDE porque las filas iguales pueden
estar en particiones distintas.
Spark redistribuye las filas según el hash de la fila entera, es decir de
las 16 columnas —no de una clave de negocio—, porque "duplicado exacto"
significa iguales en todas ellas.
Lo que garantiza que dos filas idénticas terminen siempre en la misma
partición, y por tanto en el mismo executor, que es la única forma de
poder compararlas.
El costo físico es que cada tarea del stage de arriba escribe sus filas a
disco local, separadas en cubetas según la partición destino que les tocó
por hash (shuffle write)
y cada tarea del stage de abajo jala por red su cubeta correspondiente
desde todos los executors, y no puede empezar a computar hasta haberlas
recolectado todas (shuffle read).

**Gold · KPI 1 — `groupBy("region","fecha")`.** Es WIDE porque las filas
de una misma combinación región-fecha están repartidas en varias
particiones, y no se puede sumar un grupo sin tenerlo completo. Spark
redistribuye por `hashpartitioning(region, fecha, 32)` para juntarlas, con
el mismo costo de shuffle write + shuffle read descrito arriba.

**Gold · KPI 2 — `groupBy("categoria","producto")`.** Es WIDE por la misma
razón: las ventas de un mismo producto están repartidas en varias
particiones. Clave: `hashpartitioning(categoria, producto, 32)`.

**Gold · KPI 2 — `Window.partitionBy("categoria")`.** Necesita un
`Exchange` propio, distinto del anterior, porque el particionamiento del
paso 1 no le sirve: con `hash(categoria, producto)` dos productos de la
misma categoría —"Audífonos" y "Teclado", ambos de Electrónica— caen en
particiones distintas, porque el hash se calculó sobre la pareja y no
sobre la categoría sola. Para rankear los 3 mejores productos de
Electrónica hay que tener todos sus productos juntos, así que Spark
vuelve a redistribuir, ahora con `hashpartitioning(categoria, 32)`. Que
ambos pasos "agrupen por categoría" no significa que compartan
particionamiento: lo que manda es la clave completa del hash.

**Gold · KPI 3 — `groupBy("categoria","canal")`.** Es WIDE por lo mismo:
los pedidos de una cohorte categoría-canal están repartidos en varias
particiones. Clave: `hashpartitioning(categoria, canal, 32)`.

**Bronze — 0 `Exchange`.** Porque ninguna de sus operaciones necesita
mirar otras filas: leer el CSV con un schema explícito, agregar
`_ingested_at` y `_source_file`, y escribir a Delta son transformaciones
fila a fila. Cada tarea procesa su bloque del archivo y escribe su
resultado sin coordinarse con nadie, así que todo el pipeline de Bronze
es NARROW y cabe en un solo stage.

**Nota sobre plan vs. ejecución.** El Spark UI registró 52
`ShuffleMapStage` para este script, no 1, porque los 5 `Exchange` de
arriba describen la *estructura* del plan, mientras el UI muestra sus
*ejecuciones*. Spark es perezoso: `df_dedup`, `df_fechas`, `df_region`
no calculan nada al declararse. Solo las acciones disparan el trabajo, y
`02_silver.py` llama unas doce (`n_bronze`, `n_dedup`, `n_sin_fecha`,
`n_valores_region`, `n_valores_canal`, `n_antes_35`, `n_despues_35`, la
escritura, y las verificaciones de time travel). Como el DataFrame no
está cacheado, cada acción vuelve a recorrer el plan desde el principio
y genera su propio `ShuffleMapStage`. De ahí la diferencia entre 1 y 52,
y de ahí también el argumento para usar `.cache()` cuando se va a
consultar el mismo DataFrame varias veces.

Vale la pena anotar además que en el Spark UI la query de escritura de
Silver (Query 18, 9 s) no muestra el `Exchange` de forma explícita:
`SaveIntoDataSourceCommand` presenta el plan lógico de su hijo, donde el
shuffle aparece como el operador lógico `Deduplicate` sobre las 16
columnas. El plan físico equivalente, obtenido con
`explain(mode="formatted")`, sí lo baja a
`HashAggregate → Exchange → HashAggregate`. Son el mismo paso en dos
niveles de representación.

**Evidencia — conteo por etapa** (de `explain(mode="formatted")` de cada script):

| Etapa | Exchange | Operación | Clave de particionamiento |
|---|---|---|---|
| Bronze | 0 | — | lectura CSV + escritura Delta, sin shuffle |
| Silver | 1 | `dropDuplicates()` (3.1) | `hashpartitioning(<las 16 columnas>, 32)` |
| Gold · KPI 1 | 1 | `groupBy("region","fecha")` | `hashpartitioning(region, fecha, 32)` |
| Gold · KPI 2 | 2 | `groupBy("categoria","producto")` | `hashpartitioning(categoria, producto, 32)` |
| | | `Window.partitionBy("categoria")` | `hashpartitioning(categoria, 32)` |
| Gold · KPI 3 | 1 | `groupBy("categoria","canal")` | `hashpartitioning(categoria, canal, 32)` |
| **Total** | **5** | | |

Plan físico de Silver (nótese que hay un solo `Exchange`, el nodo 4):

```
AdaptiveSparkPlan (7)
+- Project (6)
   +- HashAggregate (5)
      +- Exchange (4)
         +- HashAggregate (3)
            +- Filter (2)
               +- Scan parquet  (1)
```

> Todo el resto de Silver — los 5 formatos de fecha, los dos mapas de
> `when()`, los casts, el filtro de `cantidad`/`precio`, el
> `regexp_extract` y el `rlike` del email — se fusiona en un solo stage
> sin shuffle.

Los dos `Exchange` del KPI 2, con sus claves distintas:

```
(4) Exchange
Arguments: hashpartitioning(categoria#41, producto#42, 32), ENSURE_REQUIREMENTS

(6) Exchange
Arguments: hashpartitioning(categoria#41, 32), ENSURE_REQUIREMENTS
```

## Pregunta 2 — Recalcular vs. filtrar `total`

Elegiste recalcular `total` desde `cantidad × precio_unit` en vez de
filtrar las filas con `total` incorrecto. ¿Cuántas filas preservaste
con esta decisión vs. filtrar directamente por `total` inválido?
¿Cuándo NO sería correcto recalcular?

→ **Recalcular preservó unas 2.700 filas más, y además corrigió ~800 que
filtrar no habría detectado siquiera.**

*Recalcular* (lo que hice): filtré solo por `cantidad > 0 AND
precio_unit > 0`, y descarté **1.237 filas** (100.000 → 98.763). Descarto
únicamente cuando falta un factor del cálculo, no cuando el resultado
almacenado está mal.

*Filtrar por `total` inválido* (la alternativa): habría descartado las
**3.959 filas** con `total` nulo, cero o negativo que reportó el
profiling — unas 2.700 más que mi estrategia. Y muchas de ellas tienen
`cantidad` y `precio_unit` perfectamente válidos: se habrían perdido
ventas reales por un error en un campo derivado.

Lo decisivo es un tercer grupo: las **~800 filas con error de escala
(×1000)**. Tienen un `total` positivo y de aspecto plausible, así que
ningún filtro sobre `total` las detecta — habrían entrado a Silver
corruptas y contaminado todos los KPIs de Gold. Recalcular no solo las
conserva: las **arregla**, porque descarta el valor almacenado y lo
reconstruye desde los factores de origen.

**Cuándo NO sería correcto recalcular.** La estrategia depende de que
`total` sea un campo *derivado*, sin información propia. Deja de ser
cierto en cuanto el monto final lo modifique algo que no está en esas dos
columnas: **descuentos y promociones, impuestos, o costos de envío**. En
ese caso `total ≠ cantidad × precio_unit` es lo correcto, no un error, y
recalcular destruiría información real de negocio — todos los pedidos
quedarían con el precio de lista y desaparecería cualquier rastro de los
descuentos aplicados.

En un pipeline real habría que confirmar con el equipo de negocio que
`total` es puramente derivado antes de tomar esta decisión. Aquí es
verificable: el dataset no tiene columnas de descuento, impuesto ni
envío, así que la identidad debe cumplirse y toda desviación es una
corrupción.

**Evidencia — traza de filas de la corrida de Silver:**

```
Filas en Bronze: 101,500
3.1 Deduplicación: 101,500 -> 100,000 filas (-1,500 duplicados)
3.2 Fechas: 0 filas sin ningún formato reconocido (se descartan)
3.5 Total: 100,000 -> 98,763 filas tras filtrar cantidad/precio inválidos
    (filtro de pedido_id nulo): 98,763 -> 98,565
Filas en Silver: 98,565
```

**Evidencia — filas corruptas de `total`, del profiling** (sobre 101.500):

| Tipo de corrupción | Filas | ¿Detectable mirando solo `total`? |
|---|---|---|
| `total` nulo | 2.571 | Sí |
| `total` negativo | 926 | Sí |
| `total` = 0 | 462 | Sí |
| **Subtotal detectable** | **3.959** | |
| Error de escala (×1000) | ~800 | **No** — es un número positivo y plausible |

## Pregunta 3 — Robustez de la normalización de región

Para la normalización de región usaste `upper(trim())` + `when()` para
aliases. ¿Qué pasaría con una variante nueva que llegue la próxima
semana (`'BOG'`, `'Bgo'`)? ¿Cómo harías el pipeline más robusto sin
tener que reescribirlo cada vez que aparece una variante nueva?

→ **Qué pasaría.** Una variante nueva como `'BOG'` no coincide con ningún
`when()` del mapa, así que cae en el `.otherwise("OTRO")` y queda contada
como región "OTRO". Si llegaran 4.000 pedidos de Bogotá escritos así,
Gold reportaría que Bogotá vendió menos y que "OTRO" creció.

Lo grave no es el error sino que es **silencioso**:

- El job no falla: `otherwise()` siempre tiene una respuesta.
- La verificación del script tampoco: sigue habiendo exactamente 6
  valores distintos, porque `'BOG'` se volvió `OTRO`, que ya era uno de
  los 6. El chequeo pasa igual.
- El dato no se ve raro: "OTRO" simplemente subió un poco.

Nadie se entera. El pipeline sigue verde y las decisiones se toman con
datos malos. Un error que revienta avisa; este no.

**Cómo hacerlo robusto.** La raíz del problema es que `otherwise("OTRO")`
mete en la misma bolsa dos cosas distintas: valores que *sí* conozco y
decidí agrupar (`N/A`, `Desconocido`) y valores que **nunca había visto**.
El primer arreglo, y el más barato, es separarlos usando un centinela
distinto:

```python
construir_mapa(F.col("region"), MAPA_REGION, "NO_RECONOCIDO")
```

Con eso `N/A` y `Desconocido` siguen mapeando explícitamente a `OTRO`
desde el diccionario, mientras que cualquier valor inesperado aterriza en
`NO_RECONOCIDO`, que es una categoría que *no debería existir*. A partir
de ahí el pipeline puede avisar: contar cuántas filas cayeron ahí y, si
superan un umbral, fallar el job o emitir una alerta con la lista de
valores nuevos. El fallo pasa de silencioso a ruidoso, que es justo lo
que uno quiere de un problema de calidad de datos.

Complementos para no reescribir código en cada variante nueva:

- **Sacar el mapa del código a una tabla de referencia** (un Delta o un
  CSV de configuración). Agregar un alias pasa de ser un cambio de código
  con despliegue a una fila nueva en una tabla.
- **Normalizar más agresivamente antes de comparar**: quitar tildes con
  `translate()` además del `upper(trim())`. Eso solo habría eliminado la
  mitad de las entradas de mi diccionario actual —todas las parejas
  con/sin tilde— y absorbería automáticamente variantes nuevas que solo
  difieran en acentuación.
- **Monitorear la cardinalidad** de `region` en Bronze: si el número de
  valores distintos crece de una semana a otra, es señal de que el
  sistema de origen cambió algo.

**Evidencia — el mapa actual cubre 18 claves normalizadas** y la
verificación del script confirmó exactamente 6 valores de salida:

```
3.3 Región: 6 valores distintos después de normalizar (debe ser 6)
3.4 Canal: 4 valores distintos después de normalizar (debe ser 4)
```

> Recuerda mencionar cómo trataste `N/A`, `NA` y `Desconocido` — los
> mapeaste explícitamente a `OTRO` en vez de dejarlos caer al
> `otherwise()`. El resultado numérico es idéntico; la diferencia es
> qué comunica el código.

## Pregunta 4 — Partición y shuffle files

Ajustaste `spark.sql.shuffle.partitions=32`. Con 101.500 filas y 32
particiones: ¿cuántas filas por partición, en promedio? ¿Qué pasaría
con el valor por defecto de 200 particiones? Calcula el número de
shuffle files que genera el MERGE con 200 particiones vs. 32, en un
clúster de 4 executors.

→ **Filas por partición**

| Configuración | Cálculo | Filas por partición |
|---|---|---|
| `shuffle.partitions = 32` (la mía) | 101.500 ÷ 32 | **3.171** |
| `shuffle.partitions = 200` (defecto) | 101.500 ÷ 200 | **507** |

**Shuffle files.** El número es `tareas_map × particiones_reduce`. Bronze
son 4 archivos Parquet, o sea 4 tareas map:

| Configuración | Cálculo | Shuffle files |
|---|---|---|
| 32 particiones | 4 × 32 | **128** |
| 200 particiones | 4 × 200 | **800** |

**Qué pasaría con el defecto de 200.** Seis veces más archivos de shuffle
para mover exactamente la misma cantidad de datos. El problema no es el
espacio —son bytes iguales repartidos en más pedazos— sino que **cada
partición trae un costo fijo** que no depende de su tamaño: planificar la
tarea, serializarla, lanzarla en un executor, abrir y cerrar su archivo,
reportar el resultado al driver.

Con 507 filas por partición ese costo fijo pasa a dominar: el executor
gasta más tiempo administrando la tarea que procesando sus filas. Y del
lado del shuffle read, cada tarea tiene que abrir 4 archivos pequeños en
vez de leer bloques grandes, que es el patrón de I/O más ineficiente.

Con 32 particiones y 4 executors, cada executor procesa unas 8
particiones de ~3.171 filas: bloques con suficiente trabajo para que el
costo de planificación se amortice, y suficiente paralelismo para
mantener todos los executors ocupados.

La regla que se deduce: `shuffle.partitions` debe fijarse en función del
paralelismo real del clúster y del volumen de datos, no dejarse en el
defecto. Los 200 de Spark están pensados para datasets mucho más grandes
que 101.500 filas; en un dataset chico son puro sobrecosto.

**Datos para el cálculo:**

| Dato | Valor |
|---|---|
| Filas en Bronze | 101.500 |
| Archivos Parquet en Bronze (= tareas map) | 4 |
| `spark.sql.shuffle.partitions` configurado | 32 |
| Valor por defecto de Spark | 200 |
| Cores por executor (m5.xlarge) | 4 vCPU |

> Recuerda que el número de shuffle files de un stage es
> `tareas_map × particiones_reduce`.

## Pregunta 5 — Benchmark Athena

Según `benchmark_resultados.md`: ¿cuál fue el ratio real de bytes
escaneados (CSV vs. Parquet)? ¿Por qué el ratio puede ser distinto del
teórico (~9x del slide de S4)? ¿Qué efecto tuvo el Z-ordering sobre los
bytes escaneados?

→ **El ratio real fue 25,89x** (775.264 bytes en CSV contra 29.948 en
Gold Parquet), casi el triple del ~9x teórico.

**Por qué es distinto del teórico.** Porque las dos consultas **no están
midiendo lo mismo**. El ~9x del slide compara el *mismo dato* en dos
formatos; aquí las tablas tienen granularidades distintas:

| | Filas | Granularidad |
|---|---|---|
| `gold_ventas_region_fecha` | 3.484 | pre-agregada por `(region, fecha)` |
| `benchmark_csv_10k` | 10.000 | pedido individual |

Gold ya trae la suma calculada desde Spark; el CSV obliga a Athena a leer
cada pedido y sumarlo en el momento. Así que el 25,89x mezcla al menos
tres efectos y no puede atribuirse solo al formato columnar:

1. **Pre-agregación** — menos filas que leer, y es probablemente el
   factor dominante.
2. **Formato columnar** — la query toca 3 columnas; Parquet lee solo
   esas, el CSV obliga a parsear la fila completa. Este es el efecto que
   el ~9x pretende medir.
3. **Predicado pushdown** — sobre Parquet, Athena descarta bloques por
   estadísticas antes de leerlos; sobre CSV no hay metadatos, hay que
   leer todo el archivo para filtrar.

Un benchmark honesto del formato exigiría comparar la *misma* tabla en
CSV y en Parquet, a la misma granularidad. Como está montado, el número
sobreestima la ventaja del formato.

**Efecto del Z-ordering.** Con este experimento **no se puede aislar**, y
conviene decirlo en vez de atribuirle el ratio. El `ZORDER BY (fecha,
region)` busca que filas con valores cercanos de esas columnas queden en
los mismos archivos, para que el `WHERE fecha >= date_add('month', -3,
current_date)` permita saltarse archivos enteros. Pero la tabla Gold son
solo ~70 KB en dos archivos Parquet: a esa escala **casi todo cabe en un
archivo**, así que no hay nada que saltarse y el Z-order no tiene margen
para actuar. La reducción observada se explica mejor por la
pre-agregación y por leer 3 columnas de 5.

Para medirlo de verdad habría que escribir la misma tabla Gold dos veces
—una con `OPTIMIZE ZORDER BY` y otra sin— y correr la misma query contra
ambas comparando `DataScannedInBytes`. Y el efecto solo sería apreciable
con un volumen donde la tabla ocupe muchos archivos.

**Una anomalía que vale la pena señalar:** el CSV fue **4× más rápido**
(528 ms contra 2.093 ms) pese a escanear 26× más bytes. Leer una tabla
Delta obliga a Athena a procesar primero el `_delta_log` para resolver
qué archivos Parquet componen la versión actual, y con datos de este
tamaño ese costo fijo domina sobre el ahorro de I/O. A escala real —GB en
vez de KB— la relación se invertiría. Es un recordatorio de que el
formato columnar optimiza volumen, no latencia, y que en datasets
pequeños la ventaja puede no aparecer en el reloj.

**Evidencia:**

| Query | Tiempo motor (ms) | Bytes escaneados | Filas de la tabla |
|---|---|---|---|
| 5.1 Gold Parquet (Z-ordered por `fecha, region`) | 2.093 | 29.948 | 3.484 (pre-agregadas) |
| 5.2 CSV sin particionar | 528 | 775.264 | 10.000 (crudas) |

**Ratio CSV / Parquet = 25,89x**

> Dos observaciones que conviene que discutas:
> 1. La comparación **no es equivalente**: Gold ya viene agregada por
>    `(region, fecha)` mientras el CSV está a nivel de pedido. El ratio
>    mezcla el efecto del formato columnar con el de la pre-agregación.
> 2. El CSV fue **4× más rápido** pese a escanear 26× más bytes — el
>    costo fijo de leer el `_delta_log` domina cuando los datos son
>    tan pequeños.

---

## Anexo — hallazgo sobre la ambigüedad de fechas

`FORMATOS_FECHA` quedó con `dd/MM/yyyy` antes de `MM/dd/yyyy`. Sobre la
muestra de 10.000 filas exportada a CSV, esto produjo:

```
total | fechas_imposibles (> 2026-06-30) | pct
10000 |                             126  | 1.26
```

El dataset solo llega hasta 2026-06-30 (`gen_dataset.py:91`), así que
esas 126 filas son fechas mal parseadas: valores escritos como
`MM/dd/yyyy` que el `coalesce()` resolvió primero como `dd/MM/yyyy`.

Ningún orden de la lista acierta el 100%: cuando el día es <= 12 las dos
formas son indistinguibles sin metadatos del origen.

## Anexo — hallazgos de la ejecución real

| # | Hallazgo | Dónde |
|---|---|---|
| 1 | El orden de columnas documentado en el TODO 1 de `01_bronze.py` no coincide con el header real del CSV (`region`, `canal` y `vendedor_id` quedan al final). Como `.schema()` mapea por posición, seguir el README habría corrompido las columnas en silencio. | `01_bronze.py:50-53` vs `gen_dataset.py:182-184, 225-226` |
| 2 | El paquete Delta del Troubleshooting #5 (`delta-spark_2.12:3.1.0`) exige Spark 3.5; el clúster corre Spark 3.4.1. El correcto es `delta-core_2.12:2.4.0`. | README Troubleshooting #5 |
| 3 | `devuelto` y `calificacion` llegan a Silver como string; `cast("double")` directo sobre `"True"` da `null`. Hay que pasar por `boolean` primero. | `03_gold.py`, KPI 1 |
| 4 | `CREATE TABLE ... USING DELTA` desde Spark registra en Glue una cáscara vacía (schema `{"fields":[]}`, location HDFS con `__PLACEHOLDER__`). Athena no puede leerla; hubo que recrear las tablas con DDL de Athena y `TBLPROPERTIES ('table_type'='DELTA')`. | `03_gold.py:130-141` |
| 5 | El clúster se creó sin el metastore de Glue; requiere `--conf spark.hadoop.hive.metastore.client.factory.class=...AWSGlueDataCatalogHiveClientFactory` en el `spark-submit`. | `create_emr.sh` |
