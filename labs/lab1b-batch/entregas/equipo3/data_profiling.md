# Data Profiling — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 2026-11-06
**Estudiantes:** Mateo García Carreño — mgarciac10@eafit.edu.co
Juan José Gomez — jjgomezv2@eafit.edu.co
Juan José Vargas — jjvargasl@eafit.edu.co
Luis Moreno Gutierrez

> Ejecución: `spark-submit 00_profiling.py` como step de EMR
> (clúster `j-0121627385817QZODBXL`, emr-6.15.0, step
> `s-049176233CM62ID9LO1J`) leyendo
> `s3a://st1630-jjgomezv2-2026/raw/ventas_colombia_raw.csv`.
> Output completo en [`evidencia/00_profiling_stdout.txt`](evidencia/00_profiling_stdout.txt).

## 1. Duplicados exactos

¿Cuántos duplicados exactos tiene el dataset?

→ **1.500 duplicados exactos** (1,48 % de las 101.500 filas): al aplicar
`dropDuplicates()` sobre las 14 columnas quedan 100.000 filas únicas.

```
=== Filas totales: 101,500 ===
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

¿Cuántos formatos de fecha distintos puedes identificar? Lista al
menos 3 con ejemplos reales del dataset (valores tal cual aparecen en
la columna `fecha`).

→ **5 formatos.** El script detecta 4 "formas" por regex, pero la forma
`NN/NN/yyyy` en realidad esconde dos formatos distintos (`dd/MM/yyyy` y
`MM/dd/yyyy`):

| Formato | Filas | Ejemplos reales |
|---|---|---|
| `yyyy-MM-dd` | 20.284 | `2026-03-14`, `2025-02-09`, `2025-12-11` |
| `yyyy/MM/dd` | 20.293 | `2025/10/11`, `2025/02/23`, `2025/01/03` |
| `dd-MM-yyyy` | 20.279 | `29-06-2026`, `17-06-2026`, `10-12-2025` |
| `dd/MM/yyyy` | ⊂ 40.644 | `18/07/2025`, `27/03/2026`, `29/10/2025` |
| `MM/dd/yyyy` | ⊂ 40.644 | `07/21/2025`, `11/17/2025`, `03/19/2025` |

De las 40.644 filas con barra `NN/NN/yyyy`: 12.202 son seguro
`dd/MM/yyyy` (primer campo > 12), 12.409 son seguro `MM/dd/yyyy`
(segundo campo > 12) y **16.033 son indistinguibles** (ambos campos
≤ 12, p. ej. `06/06/2025`, `04/11/2025`). Para esas, el orden del
`coalesce()` en Silver decide la interpretación.

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

=== Ejemplos de 'fecha' por patrón ===
  yyyy-MM-dd                             -> ['2026-03-14', '2025-02-09', '2025-12-11', '2026-01-21']
  yyyy/MM/dd                             -> ['2025/10/11', '2025/02/23', '2025/01/03', '2025/06/18']
  dd/MM/yyyy o MM/dd/yyyy (ambiguo)      -> ['07/21/2025', '11/17/2025', '08/06/2025', '06/06/2025']
  dd-MM-yyyy                             -> ['11-02-2025', '29-06-2026', '17-06-2026', '10-12-2025']

# Desglose del grupo ambiguo (pandas sobre el mismo CSV, semilla fija):
barra NN/NN/yyyy total: 40644
  primer campo >12 (seguro dd/MM/yyyy): 12202  ['18/07/2025', '18/04/2025', '27/03/2026']
  segundo campo >12 (seguro MM/dd/yyyy): 12409 ['03/19/2025', '06/19/2026', '01/20/2026']
  ambos <=12 (indistinguibles): 16033
```

## 3. Variantes de "Bogotá"

¿Cuántas variantes de "Bogotá" existen en la columna `region`? Lístalas
todas con su conteo.

→ **8 variantes** (38.603 filas en total). Dos de ellas solo se
distinguen por espacios invisibles en el `.show()` (`'Bogota '` con
espacio al final y `' Bogotá'` con espacio al inicio):

| Variante (literal) | Filas |
|---|---|
| `BOGOTÁ` | 5.017 |
| `'Bogota '` (espacio final) | 4.956 |
| `bogota` | 4.894 |
| `BTA` | 4.803 |
| `Bta` | 4.796 |
| `BOGOTA` | 4.759 |
| `' Bogotá'` (espacio inicial) | 4.701 |
| `Bogotá` | 4.677 |

En total `region` tiene **35 valores distintos** (5 variantes con espacios
sobrantes: `'Bogota '`, `' Bogotá'`, `'Medellin '`, `'cali '`, `' Cali'`).

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

→ **5 variantes** (35.571 filas): `App Móvil` (7.198), `móvil` (7.158),
`app movil` (7.121), `APP MOVIL` (7.090), `APP_MOVIL` (7.004). En total
`canal` tiene **20 valores distintos** (5 por cada uno de los 4 canales).

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

→ **3,90 %** (3.959 de 101.500 filas): 2.571 nulos + 926 negativos +
462 ceros. Además el `max` de 3.893.000.000 y una media de ~3,99 M
(cuando el precio unitario máximo es 799.900 y la cantidad máxima 5)
muestran que hay totales con error de escala, no solo nulos/negativos.

```
=== Estadísticas de 'total' ===
+------------------+-------+----------------+-----+---------+-----+
|min               |max    |mean            |nulos|negativos|ceros|
+------------------+-------+----------------+-----+---------+-----+
|-49989.55707293571|3.893E9|3986873.59396779|2571 |926      |462  |
+------------------+-------+----------------+-----+---------+-----+

=== Estadísticas de 'precio_unit' ===
+---------+--------+---------+
|min      |max     |negativos|
+---------+--------+---------+
|-741400.0|799900.0|641      |
+---------+--------+---------+

=== Estadísticas de 'cantidad' ===
+----+---+---------------+
|min |max|cero_o_negativo|
+----+---+---------------+
|-5.0|5.0|622            |
+----+---+---------------+
```

## 6. Tipo de dato de `vendedor_id`

¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente en
todas las filas?

→ Se lee como **string** y **no es consistente**: conviven 3 formatos
para el mismo concepto (un id numérico de vendedor): entero puro
(`7315`, 69.592 filas · 68,6 %), prefijado `VEN-XXXX` (`VEN-3019`,
28.056 · 27,6 %) y mixto con `v` minúscula (`v8246`, 3.852 · 3,8 %).
En los tres casos la parte útil son los dígitos.

```
=== Tipos detectados en 'vendedor_id' ===
+-------------+-----+
|tipo_vendedor|count|
+-------------+-----+
|entero       |69592|
|prefijado    |28056|
|mixto        |3852 |
+-------------+-----+

Muestra de valores 'mixto':
+-----------+
|vendedor_id|
+-----------+
|v8246      |
|v1365      |
|v7484      |
|v4032      |
...
```

Adicional — `email_cliente`: 144 nulos y 1.175 con formato inválido
(`valentina.diaz895 en outlook.com`, `camila.perez836@dominio`,
`laura.martinez33@`, `camila.martinez762outlook.com`).

## 7. Regla de negocio para `total`

¿Qué regla de negocio permite detectar errores en `total`?

→ `total = cantidad × precio_unit`, con `cantidad > 0` y
`precio_unit > 0`. Cualquier fila donde `total` no cuadre con ese
producto (nulo, ≤ 0, o con error de escala como los ~3.893 M) es
inconsistente. Ejemplo real del output: `PED-025040` tiene
`cantidad=2`, `precio_unit=-52100` y `total=104200` — el total "cuadra"
en valor absoluto pero el precio es negativo, así que también hay que
validar los factores, no solo el resultado.

## 8. Resumen para ti mismo

Antes de pasar a la Parte 2 (Bronze), resume en 3-4 líneas qué
decisiones de limpieza vas a tener que tomar en Silver a partir de lo
que encontraste aquí. No hace falta que sean las decisiones finales —
es tu plan de partida.

→ En Silver quitar los 1.500 duplicados y normalizar region y canal (trim, minúsculas, sin tildes) con un mapeo a un valor canónico, llevando también Desconocido, otro y N/A a una sola categoría. Para fecha parseo con un coalesce() de los cinco formatos, y tengo que decidir cómo tratar los 16.033 casos ambiguos entre dd/MM/yyyy y MM/dd/yyyy, dejando la regla documentada. En vendedor_id me quedo solo con los dígitos, y en total valido que cantidad y precio_unit sean positivos y recalculo cantidad × precio_unit; lo que no cuadre lo corrijo o lo mando a cuarentena.
