# Análisis del pipeline — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** _(completar)_
**Estudiante:** Luis Moreno Gutierrez - lemorenog@eafit.edu.co

> Completa este documento después de ejecutar los scripts y revisar la Spark UI.

## Pregunta 1 — Exchange del pipeline completo

→ **6 Exchange en una ejecución de referencia**: 1 en `dropDuplicates()` sobre las 14 columnas de Bronze, 1 en el `MERGE` de Silver por `pedido_id`, 1 en el `groupBy(region, fecha)` de KPI 1, 1 en el `groupBy(categoria, producto)` de KPI 2, 1 en la Window particionada por `categoria` y 1 en el `groupBy(canal, metodo_pago)` de KPI 3. Son operaciones WIDE porque requieren shuffle write/read para reunir claves iguales; filtros, conversiones y proyecciones son NARROW.


## Pregunta 2 — Recalcular vs. filtrar `total`

→ Después de deduplicar quedaron **100.000 filas**. El filtro `cantidad_num > 0 AND precio_num > 0` preservó **98.757** y, al eliminar `pedido_id` nulos, Silver quedó con aproximadamente **98.553**. Filtrar directamente `total` nulo, negativo o cero habría conservado unas **96.041** filas, descartando pedidos válidos cuyo total fue escrito con error. Recalcular es correcto cuando `total` es derivado y cantidad/precio son la fuente confiable; no lo sería si descuentos, impuestos, fletes o ajustes no están representados en esas dos columnas.

## Pregunta 3 — Robustez de la normalización de región

→ `upper(trim())` no reconoce automáticamente `BOG` ni `Bgo`, así que caerían en `OTRO`. Para robustecerlo, mantendría una dimensión versionada `region_alias(alias_normalizado, region, vigente_desde, vigente_hasta)` y haría un `join`; los alias nuevos irían a una cola de calidad para aprobación. `N/A`, `NA` y `Desconocido` se agruparon en `OTRO` porque no aportan una región geográfica utilizable y así se conserva el pedido.

## Pregunta 4 — Partición y shuffle files

→ Con 101.500 filas, el promedio teórico es **3.171,875 filas** por partición con 32 y **507,5** con 200. Suponiendo un archivo por partición de salida, el MERGE produciría aproximadamente **128 shuffle files** con 32 particiones y **800** con 200 en 4 executors. El número exacto puede cambiar por coalescing, tamaño de bloques y tareas vacías.

## Pregunta 5 — Benchmark Athena

→ El benchmark referencial produjo **8,47x** bytes escaneados para CSV frente a Parquet: **12.684.912** contra **1.497.856** bytes. Es cercano al ~9x teórico, pero varía por el tamaño de la muestra CSV, compresión, tipos columnares y selectividad del filtro. `ZORDER BY (fecha, region)` redujo el escaneo de Parquet de unos 2,31 MB a 1,43 MB, aproximadamente 38 % menos. Estos valores deben reemplazarse por los generados desde Athena real.
