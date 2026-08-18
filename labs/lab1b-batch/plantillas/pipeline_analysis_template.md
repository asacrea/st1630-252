# Análisis del pipeline — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Copia este archivo a tu carpeta de entrega como `pipeline_analysis.md`
> y complétalo después de correr los 4 scripts (`01_bronze.py` a
> `04_athena_benchmark.py`) y de revisar Spark UI (Parte 3.8 del lab).

## Pregunta 1 — Exchange del pipeline completo

¿Cuántos `Exchange` tiene el pipeline completo (Bronze → Silver →
Gold)? Identifica a qué operación corresponde cada uno y explica en
términos físicos (shuffle write, shuffle read) por qué esa operación es
WIDE.

→ [tu respuesta aquí]

## Pregunta 2 — Recalcular vs. filtrar `total`

Elegiste recalcular `total` desde `cantidad × precio_unit` en vez de
filtrar las filas con `total` incorrecto. ¿Cuántas filas preservaste
con esta decisión vs. filtrar directamente por `total` inválido?
¿Cuándo NO sería correcto recalcular?

→ [tu respuesta aquí]

## Pregunta 3 — Robustez de la normalización de región

Para la normalización de región usaste `upper(trim())` + `when()` para
aliases. ¿Qué pasaría con una variante nueva que llegue la próxima
semana (`'BOG'`, `'Bgo'`)? ¿Cómo harías el pipeline más robusto sin
tener que reescribirlo cada vez que aparece una variante nueva?

→ [tu respuesta aquí]

## Pregunta 4 — Partición y shuffle files

Ajustaste `spark.sql.shuffle.partitions=32`. Con 101.500 filas y 32
particiones: ¿cuántas filas por partición, en promedio? ¿Qué pasaría
con el valor por defecto de 200 particiones? Calcula el número de
shuffle files que genera el MERGE con 200 particiones vs. 32, en un
clúster de 4 executors.

→ [tu respuesta aquí]

## Pregunta 5 — Benchmark Athena

Según `benchmark_resultados.md`: ¿cuál fue el ratio real de bytes
escaneados (CSV vs. Parquet)? ¿Por qué el ratio puede ser distinto del
teórico (~9x del slide de S4)? ¿Qué efecto tuvo el Z-ordering sobre los
bytes escaneados?

→ [tu respuesta aquí]
