# Análisis del pipeline — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** _23/08/2026_
**Estudiante:** _Nathalia Cardoza (`nvcardozaa@eafit.edu.co`), Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango_

## Pregunta 1 — Exchange del pipeline completo

¿Cuántos `Exchange` tiene el pipeline completo (Bronze → Silver →
Gold)? Identifica a qué operación corresponde cada uno y explica en
términos físicos (shuffle write, shuffle read) por qué esa operación es
WIDE.

En Gold, el patrón de plan físico (HashAggregate → Exchange → HashAggregate) se repite para cada operación de agregación. Con 3 KPIs (4.1: 1 groupBy, 4.2: 1 groupBy + 1 Window, 4.3: 1 groupBy), se generan al menos 4 operaciones que requieren shuffle: 3 por los groupBy de cada KPI, más 1 por la Window del KPI 2 (que particiona por categoria, una clave distinta a la del groupBy anterior, por lo que no puede reutilizar el shuffle previo).
Total del pipeline: 0 (Bronze) + 1 (Silver, confirmado con .explain()) + 4 (Gold, confirmado por el patrón repetido en los logs de ejecución) = 5 Exchange.
El plan físico muestra exactamente 1 Exchange correspondiente al dropDuplicates() de la Parte 3.1

**Evidencia — plan físico de la escritura a Silver:**

```
== Physical Plan ==
AdaptiveSparkPlan (7)
+- Project (6)
   +- HashAggregate (5)
      +- Exchange (4)
         +- HashAggregate (3)
            +- Filter (2)
               +- Scan parquet  (1)

(2) Filter
Condition : (...)

(3) HashAggregate
Keys [16]: [...]

(4) Exchange
Arguments: hashpartitioning(..., 32), ENSURE_REQUIREMENTS

(5) HashAggregate
...

(6) Project
...

(7) AdaptiveSparkPlan
Arguments: isFinalPlan=false
```


## Pregunta 2 — Recalcular vs. filtrar `total`

Elegiste recalcular `total` desde `cantidad × precio_unit` en vez de
filtrar las filas con `total` incorrecto. ¿Cuántas filas preservaste
con esta decisión vs. filtrar directamente por `total` inválido?
¿Cuándo NO sería correcto recalcular?

3.5 Total: 100,000 -> 98,763 filas
Al recalcular total_silver desde cantidad × precio_unit (filtrando solo cantidad>0 y precio>0), se preservaron 98,763 filas de las 100,000 que llegaron después de deduplicar. Esto significa que se descartaron 1,237 filas (1.24%) por tener cantidad/precio inválidos (menos radical que si hubiera filtrado por total del raw inválido, donde sabíamos por el profiling que ~3.9% del total original <= 0 o nulo). 
- Recalcular preserva más datos porque separa dos problemas distintos: un total corrupto en el raw no significa que cantidad y precio_unit también estén corruptas; filtrar por total habría descartado filas rescatables.
Recalcular NO sería correcto si cantidad o precio_unit fueran poco confiables (ej. si vinieran de un sistema con errores de captura), o si total incluyera información que no se puede derivar solo del producto (descuentos, impuestos, cargos adicionales) — en ese caso, recalcular perdería esa información de negocio en vez de solo corregir un error de transcripción.

## Pregunta 3 — Robustez de la normalización de región

Para la normalización de región usaste `upper(trim())` + `when()` para
aliases. ¿Qué pasaría con una variante nueva que llegue la próxima
semana (`'BOG'`, `'Bgo'`)? ¿Cómo harías el pipeline más robusto sin
tener que reescribirlo cada vez que aparece una variante nueva?

Se movería MAPA_REGION/MAPA_CANAL fuera del código hacia una fuente externa (un archivo JSON en S3, o una tabla de referencia en el catálogo) haria que el pipeline lea al arrancar, en vez de tenerlo hardcodeado. Así, agregar una variante nueva ('BOG') es una operación de datos (editar el archivo/tabla), no un despliegue de código nuevo.

## Pregunta 4 — Partición y shuffle files

Ajustaste `spark.sql.shuffle.partitions=32`. Con 101.500 filas y 32
particiones: ¿cuántas filas por partición, en promedio? ¿Qué pasaría
con el valor por defecto de 200 particiones? Calcula el número de
shuffle files que genera el MERGE con 200 particiones vs. 32, en un
clúster de 4 executors.

- 101,500 filas / 32 particiones ≈ 3,172 filas por partición en promedio
- Con 200 particiones (default): 101,500 / 200 ≈ 508 filas por partición
Dejar el valor por defecto causaría Over-partitioning (sobre-particionamiento). Para un volumen de 101.500 filas, 200 particiones es un exceso crítico. Procesar bloques de aprox 500 filas genera un alto costo de serialización, planificación de tareas y coordinación en el Driverp; asi el pipeline se vuelve ineficiente porque Spark pasa más tiempo gestionando las micro-tareas que procesando datos.
- El total de shuffle files generados por el MERGE se calcula multiplicando las tareas de origen (M) por las particiones de destino (N). Sabiendo que los datos de entrada provienen de 4 archivos en la capa Bronze (M = 4) y que el clúster cuenta con 4 executors:
Configuración optimizada (32 particiones): 4 * 32 = 128 shuffle files.
Configuración por defecto (200 particiones): 4 * 200 = 800 shuffle files.

## Pregunta 5 — Benchmark Athena

Según `benchmark_resultados.md`: ¿cuál fue el ratio real de bytes
escaneados (CSV vs. Parquet)? ¿Por qué el ratio puede ser distinto del
teórico (~9x del slide de S4)? ¿Qué efecto tuvo el Z-ordering sobre los
bytes escaneados?

El ratio real de bytes escaneados fue 25.89x (Parquet: 29,935 bytes vs. CSV: 775,068 bytes), notablemente mayor al ~9x teórico visto en el slide de S4. Esto se debe a que la comparación no es "Parquet plano vs. CSV plano" como en el ejemplo teórico, sino que la tabla Gold combina dos optimizaciones simultáneas:
- Los datos ya están agregados por región/fecha en vez de a nivel de pedido individual, reduciendo drásticamente el volumen físico
- Y tiene Z-ordering aplicado sobre region y fecha, que permite a Athena descartar archivos completos vía predicate pushdown al filtrar por esas columnas. 
El CSV de comparación, en cambio, tiene datos a nivel de fila sin ningún tipo de optimización de layout. El efecto combinado de agregación + Z-order explica por qué el ratio superó ampliamente el ~9x que solo mide la ventaja de un formato columnar sobre uno de filas.
