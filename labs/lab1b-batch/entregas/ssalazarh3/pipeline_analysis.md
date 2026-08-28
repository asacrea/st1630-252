# Análisis del pipeline — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 23 de agosto de 2026

**Estudiantes:** Sebastian Salazar Henao — ssalazarh3@eafit.edu.co, Andres Felipe Velez Alvarez - afveleza@eafit.edu.co, Samuel Samper Cardona - ssamperc@eafit.edu.co, Hellen Yanes Doria - hyanesd@eafit.edu.co

## Pregunta 1 — Exchange del pipeline completo

¿Cuántos `Exchange` tiene el pipeline completo (Bronze → Silver →
Gold)? Identifica a qué operación corresponde cada uno y explica por
qué es WIDE.

→ El pipeline implementado contiene **7 operaciones WIDE principales
que pueden producir `Exchange`**:

1. **Deduplicación de Silver:** `dropDuplicates()` redistribuye las
   filas por las columnas de negocio para reunir los duplicados.
2. **MERGE de Silver:** compara `pedido_id` entre la tabla Delta y los
   datos nuevos; físicamente se comporta como un join por clave.
3. **KPI de ventas por región y fecha:** el `groupBy(region, fecha)`
   reúne todos los pedidos del mismo grupo.
4. **Ventas por producto:** el `groupBy(categoria, producto)` agrupa los
   pedidos antes de sumar `total_silver`.
5. **Ventana del top 3:** Spark vuelve a particionar por `categoria` y
   ordena por `ventas_producto` para aplicar `rank()`.
6. **KPI de cohortes:** el `groupBy(canal, metodo_pago)` reúne las filas
   de cada cohorte.
7. **OPTIMIZE con ZORDER:** Delta reescribe y reorganiza físicamente los
   datos por `region` y `fecha`.

En cada caso, las tareas anteriores hacen **shuffle write**, es decir,
escriben bloques clasificados por la clave de salida. Después existe
una barrera entre etapas y las tareas siguientes hacen **shuffle read**
desde distintos executors. Como una fila de salida depende de filas que
pueden estar en otras particiones, la operación es WIDE.

El número exacto de nodos visibles puede ser mayor si se cuentan las
acciones auxiliares de verificación (`distinct().count()`), o diferente
si Adaptive Query Execution combina particiones o el `MERGE` utiliza
broadcast. Para el análisis se cuentan los siete shuffles principales
del pipeline y no se duplican los planes inicial y final de AQE.

## Pregunta 2 — Recalcular vs. filtrar `total`

Elegiste recalcular `total` desde `cantidad × precio_unit` en vez de
filtrar las filas con `total` incorrecto. ¿Cuántas filas preservaste y
cuándo no sería correcto recalcular?

→ Después de eliminar los 1.500 duplicados quedaron **100.000 filas
únicas**. En ellas había 610 cantidades inválidas, 633 precios inválidos
y 6 filas donde coincidían ambos problemas:

`610 + 633 - 6 = 1.237 filas descartadas`

Al validar los operandos y recalcular el total se conservaron:

`100.000 - 1.237 = 98.763 filas`

Si se hubiera filtrado directamente el `total` original cuando era
nulo o menor o igual a cero, se habrían conservado **96.096 filas**.
Por tanto, recalcular permitió preservar **2.667 filas adicionales**:

`98.763 - 96.096 = 2.667 filas preservadas`

```text
3.1 Deduplicación: 101,500 -> 100,000 filas (-1,500 duplicados)
3.5 Total: 100,000 -> 98,763 filas tras filtrar cantidad/precio inválidos
Filtro alternativo por total nulo o <= 0: 96,096 filas
```

No sería correcto recalcular si el total incluye impuestos,
descuentos, cupones, envío, propinas, comisiones, conversión de moneda
o ajustes manuales que no aparecen en `cantidad` y `precio_unit`.
Tampoco se debe recalcular si esos dos operandos son poco confiables o
si el total original es un valor contractual o contable auditado. En
esos casos se conserva el dato original, se marca la inconsistencia y
se envía a revisión.

## Pregunta 3 — Robustez de la normalización de región

Para la normalización de región usaste `upper(trim())` + `when()` para
aliases. ¿Qué pasaría con una variante nueva como `BOG` o `Bgo`?

→ Una variante nueva no coincidiría con el mapa y, con la implementación
actual, terminaría clasificada como `OTRO`. El pipeline no fallaría,
pero el pedido quedaría asignado a una región incorrecta y podría
distorsionar los KPIs.

Para hacerlo más robusto separaría los aliases del código mediante una
tabla Delta de referencia, por ejemplo `dim_alias_region`, con las
columnas `alias_normalizado`, `region_canonica`, `activo` y
`fecha_actualizacion`. Silver aplicaría primero `upper(trim())` y una
normalización de acentos y separadores, y después haría un join con esa
tabla. Agregar `BOG → BOGOTÁ` requeriría insertar una fila en la
dimensión, sin modificar el script.

También conservaría `region_original`, agregaría una bandera
`region_reconocida` y enviaría los aliases desconocidos a una tabla de
cuarentena. La similitud de texto puede sugerir candidatos, pero la
asignación definitiva debería validarse para evitar alterar los
indicadores de negocio con coincidencias incorrectas.

## Pregunta 4 — Partición y shuffle files

Ajustaste `spark.sql.shuffle.partitions=32`. Con 101.500 filas y 32
particiones: ¿cuántas filas hay por partición? ¿Qué ocurriría con 200?

→ Con 32 particiones:

`101.500 / 32 = 3.171,875 ≈ 3.172 filas por partición`

Con las 200 particiones predeterminadas:

`101.500 / 200 = 507,5 ≈ 508 filas por partición`

Para este volumen, 200 particiones producirían muchas tareas pequeñas;
el costo de planificación, serialización y coordinación podría ser
mayor que el trabajo útil.

En un clúster de cuatro executors, cada `Exchange` produciría
lógicamente:

- **200 particiones:** 50 tareas o salidas por executor.
- **32 particiones:** 8 tareas o salidas por executor.

La reducción es del **84 %**:

`(200 - 32) / 200 × 100 = 84 %`

Si el plan del `MERGE` muestra dos Exchanges, uno por cada lado del
join, serían **400 particiones lógicas frente a 64**, o aproximadamente
100 frente a 16 por executor. El número físico exacto de archivos puede
variar por el shuffle manager y por Adaptive Query Execution; la
configuración controla particiones lógicas, no garantiza exactamente
el mismo número de archivos finales en S3.

## Pregunta 5 — Benchmark Athena

Según `benchmark_resultados.md`, ¿cuál fue el ratio real de bytes
escaneados y por qué puede ser diferente del valor teórico?

→ El benchmark ejecutado produjo un ratio real de:

`CSV / Parquet = 25,88x`

Es decir, Athena escaneó aproximadamente **25,88 veces más bytes** al
consultar la muestra CSV sin particionar que al consultar Gold en
Delta/Parquet. El valor es mayor que el orden de magnitud teórico de
aproximadamente 9x porque Parquet es columnar, permite leer solo las
columnas necesarias y utiliza compresión y estadísticas. Además, Gold
ya está agregado por región y fecha, mientras que el CSV conserva
pedidos individuales; por eso la comparación mide simultáneamente el
efecto del formato y el de la preagregación.

El tamaño pequeño de la muestra, el número y tamaño de archivos, la
selectividad del rango de fechas y el overhead mínimo de Athena también
pueden modificar el ratio. Por ello, el resultado no debe interpretarse
como una comparación pura entre CSV y Parquet sobre datos idénticos.

El Z-ordering agrupó físicamente valores cercanos de `region` y `fecha`
y puede favorecer el data skipping cuando el motor aprovecha las
estadísticas de archivos. Sin embargo, con este benchmark no se puede
cuantificar su efecto de manera aislada porque no se ejecutó la misma
consulta sobre la misma tabla Parquet antes y después de `ZORDER`.
Parte de la reducción proviene también de Parquet, la compresión,
`OPTIMIZE` y la preagregación de Gold.
