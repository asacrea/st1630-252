# Data Profiling — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 23/08/2026
**Estudiante:** Sebastian Andres Medina Cabezas - samedinac@eafit.edu.co

## 1. Duplicados exactos

¿Cuántos duplicados exactos tiene el dataset?

→ El dataset tiene 1,500 duplicados exactos, equivalentes al 1.48% del total de filas (101,500).

```
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

¿Cuántos formatos de fecha distintos puedes identificar? Lista al
menos 3 con ejemplos reales del dataset (valores tal cual aparecen en
la columna `fecha`).

→ dd/MM/yyyy o MM/dd/yyyy (ambiguo): 40,644 filas
yyyy/MM/dd: 20,293 filas
yyyy-MM-dd: 20,284 filas
dd-MM-yyyy: 20,279 filas

=== Muestra: 3 filas con pedido_id nulo ===
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
|pedido_id|fecha     |categoria|producto|cantidad|precio_unit|total   |email_cliente              |metodo_pago    |devuelto|calificacion|region      |canal    |vendedor_id|
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
|null     |04/11/2025|Alimentos|Aceite  |1.0     |5400.0     |5400.0  |sara.castro393@outlook.com |efectivo       |False   |3           | Bogotá     |físico   |VEN-3019   |
|null     |2025/11/20|Hogar    |Cafetera|2.0     |249900.0   |499800.0|andres.lopez878@yahoo.com  |tarjeta_debito |False   |4           |bogota      |APP MOVIL|7315       |
|null     |29/10/2025|Alimentos|Aceite  |2.0     |15900.0    |31800.0 |santiago.gomez740@yahoo.com|tarjeta_credito|False   |3           |barranquilla|App Móvil|3324       |
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+

```
=== Formatos de fecha detectados (top 10 por patrón) ===
+---------------------------------+-----+
|patron_fecha                     |count|
+---------------------------------+-----+
|dd/MM/yyyy o MM/dd/yyyy (ambiguo)|40644|
|yyyy/MM/dd                       |20293|
|yyyy-MM-dd                       |20284|
|dd-MM-yyyy                       |20279|
+---------------------------------+-----+

=== Muestra: 3 filas con pedido_id nulo ===
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
|pedido_id|fecha     |categoria|producto|cantidad|precio_unit|total   |email_cliente              |metodo_pago    |devuelto|calificacion|region      |canal    |vendedor_id|
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
|null     |04/11/2025|Alimentos|Aceite  |1.0     |5400.0     |5400.0  |sara.castro393@outlook.com |efectivo       |False   |3           | Bogotá     |físico   |VEN-3019   |
|null     |2025/11/20|Hogar    |Cafetera|2.0     |249900.0   |499800.0|andres.lopez878@yahoo.com  |tarjeta_debito |False   |4           |bogota      |APP MOVIL|7315       |
|null     |29/10/2025|Alimentos|Aceite  |2.0     |15900.0    |31800.0 |santiago.gomez740@yahoo.com|tarjeta_credito|False   |3           |barranquilla|App Móvil|3324       |
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
```

## 3. Variantes de "Bogotá"

¿Cuántas variantes de "Bogotá" existen en la columna `region`? Lístalas
todas con su conteo.

→ Hay 8 variantes de Bogotá en region:

BOGOTÁ: 5,017
Bogota: 4,956
bogota: 4,894
BTA: 4,803
Bta: 4,796
BOGOTA: 4,759
espacio+Bogotá ( Bogotá): 4,701
Bogotá: 4,677

```
=== Valores únicos de 'region' (ordenados por frecuencia) ===
+------------+-----+
|region      |count|
+------------+-----+
|BOGOTÁ      |5017 |
|Bogota      |4956 |
|bogota      |4894 |
|BTA         |4803 |
|Bta         |4796 |
|BOGOTA      |4759 |
| Bogotá     |4701 |
|Bogotá      |4677 |
```

## 4. Variantes de "app_movil"

¿Cuántas variantes de "app_movil" existen en la columna `canal`?
Lístalas todas con su conteo.

→ Hay 5 variantes de app_movil en canal:

App Móvil: 7,198
móvil: 7,158
app movil: 7,121
APP MOVIL: 7,090
APP_MOVIL: 7,004

```
=== Valores únicos de 'canal' (ordenados por frecuencia) ===
+-------------+-----+
|canal        |count|
+-------------+-----+
|App Móvil    |7198 |
|móvil        |7158 |
|app movil    |7121 |
|APP MOVIL    |7090 |
|APP_MOVIL    |7004 |
```

## 5. `total` <= 0 o nulo

¿Qué porcentaje de filas tiene `total <= 0` o nulo?

→ nulos: 2,571
negativos: 926
ceros: 462
Filas con total <= 0 o nulo = 2,571 + 926 + 462 = 3,959.
Porcentaje = 3,959 / 101,500 = 3.90%.

```
=== Nulos por columna ===
  total                   2,571  (2.53%)

=== Estadísticas de 'total' ===
+------------------+-------+----------------+-----+---------+-----+
|min               |max    |mean            |nulos|negativos|ceros|
+------------------+-------+----------------+-----+---------+-----+
|-49989.55707293571|3.893E9|3986873.59396779|2571 |926      |462  |
+------------------+-------+----------------+-----+---------+-----+
```

## 6. Tipo de dato de `vendedor_id`

¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente en
todas las filas?

→ No es consistente. En la muestra aparecen al menos dos formas:

solo dígitos (ejemplo: 7315, 3324)
prefijado con VEN- (ejemplo: VEN-3019)

```
=== Muestra: 3 filas con pedido_id nulo ===
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
|pedido_id|fecha     |categoria|producto|cantidad|precio_unit|total   |email_cliente              |metodo_pago    |devuelto|calificacion|region      |canal    |vendedor_id|
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
|null     |04/11/2025|Alimentos|Aceite  |1.0     |5400.0     |5400.0  |sara.castro393@outlook.com |efectivo       |False   |3           | Bogotá     |físico   |VEN-3019   |
|null     |2025/11/20|Hogar    |Cafetera|2.0     |249900.0   |499800.0|andres.lopez878@yahoo.com  |tarjeta_debito |False   |4           |bogota      |APP MOVIL|7315       |
|null     |29/10/2025|Alimentos|Aceite  |2.0     |15900.0    |31800.0 |santiago.gomez740@yahoo.com|tarjeta_credito|False   |3           |barranquilla|App Móvil|3324       |
+---------+----------+---------+--------+--------+-----------+--------+---------------------------+---------------+--------+------------+------------+---------+-----------+
only showing top 3 rows
```

## 7. Regla de negocio para `total`

¿Qué regla de negocio permite detectar errores en `total`?

→ La regla de validación es:
total = cantidad × precio_unit, con cantidad > 0 y precio_unit > 0.

## 8. Resumen para ti mismo

Antes de pasar a la Parte 2 (Bronze), resume en 3-4 líneas qué
decisiones de limpieza vas a tener que tomar en Silver a partir de lo
que encontraste aquí. No hace falta que sean las decisiones finales —
es tu plan de partida.

→ En Silver voy a:

deduplicar filas exactas,
normalizar fecha, region y canal,
castear numéricos y recalcular total usando cantidad × precio_unit en vez de confiar en el raw,
estandarizar vendedor_id y validar email_cliente para evitar errores de nulos o emails sin @
