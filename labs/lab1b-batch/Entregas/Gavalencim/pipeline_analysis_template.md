# Análisis del pipeline — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 22/08/2026
**Estudiante:** Emmanuel Alvarez Castrillon correo: ealvarezc1@eafit.edu.co

> Copia este archivo a tu carpeta de entrega como `pipeline_analysis.md`
> y complétalo después de correr los 4 scripts (`01_bronze.py` a
> `04_athena_benchmark.py`) y de revisar Spark UI (Parte 3.8 del lab).

## Pregunta 1 — Exchange del pipeline completo

¿Cuántos `Exchange` tiene el pipeline completo (Bronze → Silver →
Gold)? Identifica a qué operación corresponde cada uno y explica en
términos físicos (shuffle write, shuffle read) por qué esa operación es
WIDE.

El pipeline completo tiene 1 Exchange visible en el plan físico que fue imprimido en la consola, asociado a la operación de dropDuplicates() que Spark ejecuta durante la deduplicación. Es una operación WIDE porque Spark debe agrupar las filas por todas sus columnas para identificar duplicados; para ello realiza un shuffle write, redistribuyendo los datos según un hashpartitioning entre las particiones, y posteriormente un shuffle read para que las filas que deben compararse queden en el mismo executor. En el plan aparece explícitamente Exchange (4) con hashpartitioning(calificacion#47, categoria#39, devuelto#46, _ingested_at#51, total#43, pedido_id#37, cantidad#41, fecha#38, _source_file#52, email_cliente#44, precio_unit#42, region#48, metodo_pago#45, vendedor_id#50, canal#49, producto#40, 32).

## Pregunta 2 — Recalcular vs. filtrar `total`

Elegiste recalcular `total` desde `cantidad × precio_unit` en vez de
filtrar las filas con `total` incorrecto. ¿Cuántas filas preservaste
con esta decisión vs. filtrar directamente por `total` inválido?
¿Cuándo NO sería correcto recalcular?

Al recalcular total a partir de cantidad × precio_unit se preservaron 86.703 filas, mientras que al filtrar directamente las filas con total inválido se habrían conservado 84.362 filas, es decir, la estrategia de recalcular permitió conservar 2.341 filas más. Esto es preferible porque el campo total del archivo original es poco confiable debido a valores nulos, negativos y errores de escala; por tanto, siempre que cantidad y precio_unit sean válidos y positivos, podemos obtener un total_silver consistente con la regla de negocio. No sería correcto recalcular si cantidad o precio_unit fueran incorrectos, ambiguos o no representaran realmente los valores utilizados para calcular el total original.

## Pregunta 3 — Robustez de la normalización de región

Para la normalización de región usaste `upper(trim())` + `when()` para
aliases. ¿Qué pasaría con una variante nueva que llegue la próxima
semana (`'BOG'`, `'Bgo'`)? ¿Cómo harías el pipeline más robusto sin
tener que reescribirlo cada vez que aparece una variante nueva?

Si la próxima semana llegara una variante nueva como 'BOG' o 'Bgo', el pipeline actual la convertiría en OTRO, porque no existe en MAPA_REGION; aunque upper(trim()) permite manejar diferencias de mayúsculas y espacios, no identifica automáticamente nuevos alias. Para hacerlo más robusto, utilizaría una tabla de correspondencias de regiones mantenida como dato de configuración, donde se almacenen las variantes y su valor canónico, y realizaría un join contra esa tabla en lugar de tener todos los alias escritos directamente en el código. Así, cuando aparezca una nueva variante solo habría que agregarla a la tabla de correspondencias, sin modificar ni reescribir el pipeline.

## Pregunta 4 — Partición y shuffle files

Ajustaste `spark.sql.shuffle.partitions=32`. Con 101.500 filas y 32
particiones: ¿cuántas filas por partición, en promedio? ¿Qué pasaría
con el valor por defecto de 200 particiones? Calcula el número de
shuffle files que genera el MERGE con 200 particiones vs. 32, en un
clúster de 4 executors.

Con 101.500 filas y 32 particiones, habría en promedio 3.171,875 filas por partición, es decir, aproximadamente 3.172 filas por partición. Con el valor por defecto de 200 particiones, serían 507,5 filas por partición en promedio, aproximadamente 508. Para el cálculo de shuffle files, con 4 executors y 200 particiones de salida, cada executor tendría 8 cores pero el número de archivos depende de las tareas y particiones de shuffle; usando la simplificación habitual de un archivo por combinación de partición de salida y executor, serían aproximadamente 200 × 4 = 800 shuffle files con 200 particiones, frente a 32 × 4 = 128 shuffle files con 32 particiones. Por tanto, reducir de 200 a 32 particiones disminuye considerablemente la cantidad potencial de archivos intermedios y el overhead de coordinación.

## Pregunta 5 — Benchmark Athena

Según `benchmark_resultados.md`: ¿cuál fue el ratio real de bytes
escaneados (CSV vs. Parquet)? ¿Por qué el ratio puede ser distinto del
teórico (~9x del slide de S4)? ¿Qué efecto tuvo el Z-ordering sobre los
bytes escaneados?

El benchmark obtuvo 774.922 bytes escaneados con CSV frente a 29.768 bytes con Parquet, dando un ratio de 774.922 / 29.768 ≈ 26,03×; es decir, el CSV escaneó aproximadamente 26 veces más datos que el Parquet. Este resultado no coincide con el teórico de ~9×, lo cual puede atribuirse al tamaño y características de la muestra, al formato de almacenamiento y, especialmente, al efecto del Z-ordering y la selectividad de la consulta: al consultar las regiones relevantes, Parquet puede aprovechar estadísticas y organización de los datos para evitar leer grandes cantidades de información, mientras que el CSV sin particionar debe leer mucho más contenido. En este caso, el Z-ordering contribuyó a reducir significativamente los bytes escaneados.
