# Data Profiling — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 23 de agosto de 2026

**Estudiantes:** Sebastian Salazar Henao — ssalazarh3@eafit.edu.co, Andres Felipe Velez Alvarez - afveleza@eafit.edu.co, Samuel Samper Cardona - ssamperc@eafit.edu.co, Hellen Yanes Doria - hyanesd@eafit.edu.co

> Documento completado después de ejecutar `scripts/00_profiling.py`
> sobre el dataset crudo y antes de aplicar las transformaciones de
> Silver.

## 1. Duplicados exactos

¿Cuántos duplicados exactos tiene el dataset?

→ El dataset tiene **1.500 duplicados exactos**, considerando todas las
columnas. Esto representa aproximadamente el **1,48 %** de las 101.500
filas.

```text
=== Filas totales: 101,500 ===
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

¿Cuántos formatos de fecha distintos puedes identificar? Lista al
menos 3 con ejemplos reales del dataset.

→ Se identificaron **5 formatos de fecha distintos**:

1. `yyyy-MM-dd`: `2025-01-10`.
2. `dd/MM/yyyy`: `18/07/2025`.
3. `dd-MM-yyyy`: `21-06-2026`.
4. `MM/dd/yyyy`: `03/19/2025`.
5. `yyyy/MM/dd`: `2025/01/23`.

El profiling por expresión regular muestra cuatro patrones porque
`dd/MM/yyyy` y `MM/dd/yyyy` tienen la misma estructura textual. Se
distinguen al inspeccionar valores no ambiguos: en `18/07/2025`, 18 no
puede ser un mes; y en `03/19/2025`, 19 no puede ser un mes.

```text
=== Formatos de fecha detectados (top 10 por patrón) ===
+------------------------------------+-----+
|patron_fecha                        |count|
+------------------------------------+-----+
|dd/MM/yyyy o MM/dd/yyyy (ambiguo)   |40644|
|yyyy/MM/dd                          |20293|
|yyyy-MM-dd                          |20284|
|dd-MM-yyyy                          |20279|
+------------------------------------+-----+
```

## 3. Variantes de "Bogotá"

¿Cuántas variantes de "Bogotá" existen en la columna `region`? Lístalas
todas con su conteo.

→ Existen **8 variantes**. En total representan **38.603 filas**. Las
comillas permiten observar los espacios iniciales o finales.

```text
'BOGOTÁ'   5,017
'Bogota '  4,956   <- espacio final
'bogota'   4,894
'BTA'      4,803
'Bta'      4,796
'BOGOTA'   4,759
' Bogotá'  4,701   <- espacio inicial
'Bogotá'   4,677
Total     38,603
```

## 4. Variantes de "app_movil"

¿Cuántas variantes de "app_movil" existen en la columna `canal`?
Lístalas todas con su conteo.

→ Existen **5 variantes** de `app_movil`. En total representan **35.571
filas**.

```text
'App Móvil'  7,198
'móvil'      7,158
'app movil'  7,121
'APP MOVIL'  7,090
'APP_MOVIL'  7,004
Total       35,571
```

## 5. `total <= 0` o nulo

¿Qué porcentaje de filas tiene `total <= 0` o nulo?

→ Hay **3.959 filas** con `total <= 0` o nulo. Esto equivale al
**3,9005 %**, aproximadamente **3,90 %** del dataset.

```text
=== Estadísticas de 'total' ===
+------------------+-------------+------------------+-----+---------+-----+
|min               |max          |mean              |nulos|negativos|ceros|
+------------------+-------------+------------------+-----+---------+-----+
|-49989.55707293571|3.893E9      |3986873.5939677902|2571 |926      |462  |
+------------------+-------------+------------------+-----+---------+-----+

total <= 0 no nulo = 926 + 462 = 1,388
total <= 0 o nulo  = 1,388 + 2,571 = 3,959
porcentaje         = (3,959 / 101,500) * 100 = 3.9005 %
```

## 6. Tipo de dato de `vendedor_id`

¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente en
todas las filas?

→ En el DataFrame de Spark, `vendedor_id` tiene tipo **`string`** porque
el CSV se leyó sin inferencia de esquema. El tipo físico es consistente,
pero el **formato del identificador no lo es**: hay valores numéricos,
valores con prefijo `VEN-` y valores mixtos como `v2665`.

```text
=== Tipos detectados en 'vendedor_id' ===
+-------------+-----+
|tipo_vendedor|count|
+-------------+-----+
|entero       |69592|
|prefijado    |28056|
|mixto        |3852 |
+-------------+-----+

Ejemplos: 9240, VEN-1610, v2665
```

## 7. Regla de negocio para `total`

¿Qué regla de negocio permite detectar errores en `total`?

→ La regla es:

`total = cantidad × precio_unit`

Un registro es inconsistente cuando el total es nulo, no positivo o no
coincide con la multiplicación. Antes de recalcularlo también se debe
verificar que `cantidad > 0` y `precio_unit > 0`, porque no es correcto
construir un total válido a partir de operandos inválidos.

## 8. Resumen para ti mismo

Antes de pasar a Silver, resume qué decisiones de limpieza tendrás que
tomar.

→ En Silver eliminaré duplicados usando las columnas de negocio,
normalizaré los cinco formatos de fecha y consolidaré las 35 variantes
de región y las 20 de canal. Conservaré únicamente filas con cantidad y
precio positivos y recalcularé `total_silver = cantidad × precio_unit`.
También unificaré `vendedor_id`, marcaré correos inválidos, convertiré
`devuelto` y `calificacion` a sus tipos correctos y descartaré claves
`pedido_id` nulas antes del `MERGE`.
