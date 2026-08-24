# Data Profiling — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 23/08/2026
**Estudiante:** _Nathalia Cardoza (`nvcardozaa@eafit.edu.co`), Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango_


## 1. Duplicados exactos

¿Cuántos duplicados exactos tiene el dataset?

1500 (el 1.48%).

```
=== Filas totales: 101,500 ===
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

¿Cuántos formatos de fecha distintos puedes identificar? Lista al
menos 3 con ejemplos reales del dataset (valores tal cual aparecen en
la columna `fecha`).

- dd/MM/yyyy
- yyyy/MM/dd
- yyyy-MM-dd

En el dataset se usan 5 formatos distintos para las fechas aunque el output muestra solo 4 filas; esto es porque uno de ellos es ambiguo (el output lo indica).

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

```

## 3. Variantes de "Bogotá"

¿Cuántas variantes de "Bogotá" existen en la columna `region`? Lístalas
todas con su conteo.

Existen 8 variantes en la forma en la que se escribe "Bogotá" en el csv, las cuales son:
- BOGOTÁ
- Bogota
- bogota
- BTA
- Bta
- BOGOTA
-  Bogotá
- Bogotá

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
|Medellín    |3487 |
|MEDELLÍN    |3444 |
|medellin    |3425 |
|Medellin    |3392 |
|MDE         |3332 |
|medellín    |3316 |
|CALI        |2598 |
|Cali        |2579 |
| Cali       |2570 |
|CLO         |2550 |
|cali        |2487 |
|cali        |2473 |
|BARRANQUILLA|2042 |
|Bquilla     |2018 |
|Barranquilla|2015 |
|BAQ         |2015 |
|barranquilla|1912 |
|BGA         |1869 |
|Bucaramanga |1842 |
|Buca        |1837 |
|bucaramanga |1830 |
|BUCARAMANGA |1734 |
|Desconocido |1665 |
|otro        |1634 |
|N/A         |1632 |
|NA          |1615 |
|OTRO        |1584 |
+------------+-----+

Total de valores distintos en 'region': 35
```

## 4. Variantes de "app_movil"

¿Cuántas variantes de "app_movil" existen en la columna `canal`?
Lístalas todas con su conteo.

Hay 5 variantes de "app_movil", son:
- App Móvil: 7198
- móvil: 7158
- app movil: 7121
- APP MOVIL: 7090
- APP_MOVIL: 7004

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
|online       |6112 |
|pagina_web   |6111 |
|WEB          |6083 |
|sitio_web    |6082 |
|Web          |6036 |
|TIENDA FISICA|5118 |
|Tienda Física|5105 |
|tienda       |5065 |
|TIENDA       |4977 |
|físico       |4893 |
|call_center  |2181 |
|llamada      |2076 |
|TELEFONO     |2054 |
|tel          |2020 |
|Teléfono     |2016 |
+-------------+-----+

Total de valores distintos en 'canal': 20
```

## 5. `total` <= 0 o nulo

¿Qué porcentaje de filas tiene `total <= 0` o nulo?

Para esta respuesta se sumó nulos, negativos y ceros (respectivamente) para después calcular el porcentaje, quedando asi:
2571+926+462=3959
3959/101500=0,039 (3,9%)

```
=== Estadísticas de 'total' ===
+------------------+-------+------------------+-----+---------+-----+
|min               |max    |mean              |nulos|negativos|ceros|
+------------------+-------+------------------+-----+---------+-----+
|-49989.55707293571|3.893E9|3986873.5939677902|2571 |926      |462  |
+------------------+-------+------------------+-----+---------+-----+

```

## 6. Tipo de dato de `vendedor_id`

¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente en
todas las filas?

Tiene datos enteros (9240), prefijados (VEN-1610) y mixtos(v2665); No es consistente, cosa que se puede envidenciar en el print del profiling, ya que son 3 tipos de identificadores distintos como se mencionó anteriormente.

```
=== Identificadores de 'tipo_vendedor' ===
+-------------+-----+
|tipo_vendedor|count|
+-------------+-----+
|entero       |69592|
|prefijado    |28056|
|mixto        |3852 |
+-------------+-----+

+-----------+
|vendedor_id|
+-----------+
|9240       |
+-----------+
only showing top 1 row
+-----------+
|vendedor_id|
+-----------+
|VEN-1610   |
+-----------+
only showing top 1 row
+-----------+
|vendedor_id|
+-----------+
|v2665      |
+-----------+
only showing top 1 row
```

## 7. Regla de negocio para `total`

¿Qué regla de negocio permite detectar errores en `total`?

El que se generen ventas con valores nulos, negativos o que no sean consistentes en el cálculo de precio_items*cantidad

## 8. Resumen para ti mismo

Antes de pasar a la Parte 2 (Bronze), resume en 3-4 líneas qué
decisiones de limpieza vas a tener que tomar en Silver a partir de lo
que encontraste aquí. No hace falta que sean las decisiones finales —
es tu plan de partida.

Considero que lo pertinente es eliminar los valores negativos, nulos o inconsistentes en ventas, estandarizar el tipo de fecha que se usará, eliminar tambien las filas duplicadas, unificar los nombres de las regiones y de los canales
