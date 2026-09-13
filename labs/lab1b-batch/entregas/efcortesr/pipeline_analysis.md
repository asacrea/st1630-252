# Analisis del pipeline - Lab 1b

**Curso:** ST1630-2026-2 - **Semana:** S5-S6 - **Fecha:** 2026-08-20  
**Estudiante:** Emmanuel Felipe Cortes Rincon - Sara Hurtado Metaute - Juan Jose Osorio - Mariana Sanchez

## Estado de ejecucion

- Dataset raw: `s3://st1630-efcortesr-2026/raw/ventas_colombia_raw.csv`.
- Cluster EMR usado: `j-09419123FVA3WGNZBANI`.
- Bronze escrito en Delta: `s3a://st1630-efcortesr-2026/bronze/pedidos`.
- Silver escrito en Delta: `s3a://st1630-efcortesr-2026/silver/pedidos`.
- Gold escrito en Delta y exportado a Parquet limpio para Athena.
- Glue Catalog verificado con: `gold_ventas_region_fecha`, `gold_top_productos_categoria`, `gold_cohortes_canal_pago` y `benchmark_csv_10k`.
- Evidencias en `entregas/efcortesr/evidencia/`.

## Pregunta 1 - Exchange del pipeline completo

En el flujo principal conte 5 operaciones WIDE que explican los `Exchange` relevantes:

| Capa | Operacion | Tipo | Evidencia |
|---|---|---|---|
| Bronze | Lectura CSV + columnas de auditoria + escritura Delta | NARROW | La ingesta conserva 101,500 filas, 1,500 duplicados y nulos del raw. No requiere redistribuir por clave. |
| Silver | `dropDuplicates()` sobre las columnas de Bronze | WIDE | `02_silver_output.txt` muestra `Exchange hashpartitioning(..., 32)` en el plan fisico y deduplicacion `101,500 -> 100,000`. |
| Silver | parseos, normalizacion, casts, filtros, `regexp_extract`, validacion email | NARROW | Son transformaciones fila a fila; el plan muestra estas columnas dentro de `Project`/`Filter`, despues del `Exchange` de deduplicacion. |
| Gold KPI 1 | `groupBy("region", "fecha")` | WIDE | `03_gold_output.txt`: KPI region/fecha produjo 3,484 filas. Agrupar por llave requiere shuffle. |
| Gold KPI 2 | `groupBy("categoria", "producto")` | WIDE | `03_gold_output.txt`: top productos produjo 15 filas; la suma por producto requiere shuffle. |
| Gold KPI 2 | `Window.partitionBy("categoria").orderBy(...)` | WIDE | La ventana necesita redistribuir/ordenar por categoria para calcular `rank`. |
| Gold KPI 3 | `groupBy("canal", "metodo_pago")` con `countDistinct` | WIDE | `03_gold_output.txt`: cohortes produjo 20 filas; la agregacion y el conteo distinto requieren shuffle. |

No cuento como pipeline principal los `Exchange` generados por acciones auxiliares de verificacion (`count`, `show`, validaciones de distintos o history), porque son jobs extra para evidencia. En el plan textual de Silver la evidencia mas clara es un `Exchange hashpartitioning(..., 32)` asociado a `dropDuplicates()`.

## Pregunta 2 - Recalcular vs. filtrar `total`

La regla de negocio usada en Silver fue:

```text
total_silver = round(cantidad * precio_unit, 2)
```

Despues de deduplicar quedaron 100,000 filas. Con la estrategia de recalcular `total` desde `cantidad` y `precio_unit`, y descartando solo `pedido_id`, `cantidad` o `precio_unit` invalidos, Silver preservo 98,565 filas. Si en cambio se hubiera filtrado por `total` crudo correcto, positivo y no nulo, solo quedarian 93,922 filas.

```text
Filas deduplicadas: 100,000
Filas preservadas recalculando total: 98,565
Filas que quedarian filtrando total raw correcto: 93,922
Diferencia preservada por recalculo: 4,643 filas
```

La decision correcta fue recalcular porque `total` es una columna derivada y venia contaminada: el profiling mostro 2,571 nulos en `total` y 3,959 filas con `total <= 0` o nulo. Si `cantidad` y `precio_unit` son validos, el pedido conserva valor analitico aunque el `total` raw este roto.

## Pregunta 3 - Robustez de la normalizacion de region

El raw tenia 35 variantes de `region`; Silver las redujo a 6 valores canonicos. La decision aplicada fue mapear ciudades conocidas a su nombre canonico y agrupar `N/A`, `NA`, `Desconocido`, `otro` y `OTRO` como `OTRO`.

```text
Valores distintos region raw: 35
Valores distintos region Silver: 6
Categoria de desconocidos/no aplicables: OTRO
```

La normalizacion usa una llave robusta antes de comparar: `trim`, `upper`, remocion de tildes y remocion de caracteres no alfanumericos. Eso cubre variantes como `Bogota`, `BOGOTA`, `Bogota`, `BTA`, espacios y mojibake parcial. Para una variante nueva como `BOG` o `Bgo`, la solucion mantenible no seria agregar `when` indefinidamente en codigo, sino mover aliases a una tabla de referencia versionada y mandar valores no reconocidos a cuarentena para revision. Asi el pipeline sigue siendo reproducible y las reglas cambian sin reescribir transformaciones.

## Pregunta 4 - Particion y shuffle files

El laboratorio configuro:

```text
Filas raw: 101,500
spark.sql.shuffle.partitions: 32
Default Spark: 200
```

Promedios teoricos:

```text
101,500 / 32  = 3,171.875 filas por particion
101,500 / 200 = 507.5 filas por particion
```

Con 32 particiones hay particiones mas grandes, pero menos overhead de scheduling y menos archivos/bloques de shuffle. Con 200 particiones, para este dataset pequeno, se generan muchas particiones pequenas y mas metadata de shuffle sin ganar paralelismo real.

Estimacion de shuffle files/buckets para un shuffle:

```text
Con 1 tarea map observada en la lectura principal: 1 * 32 = 32 buckets vs. 1 * 200 = 200 buckets
Con 4 tareas map como escenario de clase: 4 * 32 = 128 buckets vs. 4 * 200 = 800 buckets
```

Es decir, usar 200 puede multiplicar aproximadamente por 6.25 los buckets/archivos intermedios frente a 32. Para 101,500 filas, `32` es mas razonable que el default `200`.

## Pregunta 5 - Benchmark Athena

Resultado real generado por `04_athena_benchmark.py`:

```text
Bytes escaneados Parquet/Gold: 29,948
Bytes escaneados CSV: 773,897
Ratio CSV / Parquet: 25.84x
QueryExecutionId Parquet: d3ced93f-5b78-4b1c-85b8-0e718b355abb
QueryExecutionId CSV: 8beb8a45-8443-47bb-9c5f-22567e691154
```

El ratio fue mayor que el ~9x teorico. No lo interpreto como una propiedad universal de Parquet contra CSV, sino como resultado de esta prueba concreta: la tabla Gold Parquet esta agregada a 3,484 filas y Athena solo necesita columnas `region`, `fecha` y `ventas_totales`; la tabla CSV tiene 10,000 filas de detalle y Athena debe escanear el archivo CSV completo aunque la query use pocas columnas. El Z-order ayuda conceptualmente para filtros por `fecha`/`region`, pero en esta exportacion limpia quedo un solo archivo Parquet, asi que el mayor efecto visible fue el formato columnar mas la agregacion previa de Gold.

Top 5 devuelto por Athena en ambas tablas:

```text
Parquet Gold: BOGOTA, MEDELLIN, CALI, BARRANQUILLA, BUCARAMANGA
CSV 10k:      BOGOTA, MEDELLIN, CALI, BARRANQUILLA, BUCARAMANGA
```
