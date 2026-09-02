# Data Profiling — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 2026-08-22

**Estudiantes:**
- Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co
- Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
- Sebastián Durán Fernández — sduranf@eafit.edu.co
- Daniel Arcila Salazar — darcilas1@eafit.edu.co

> Ejecutado con `spark-submit s3://st1630-jjdiazr-2026/scripts/00_profiling.py`
> como step `s-0506660I1RU00BESNTI` en el clúster EMR `j-08749873HH1YI3USSKPI`
> (emr-6.15.0, Spark 3.4.1). Salida completa en `profiling_output.txt`.

## 1. Duplicados exactos

¿Cuántos duplicados exactos tiene el dataset?

→ 1.500 duplicados exactos, un 1,48% de las 101.500 filas. Son filas
idénticas en las 14 columnas, no solo repeticiones de `pedido_id`.

```
=== Filas totales: 101,500 ===
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

¿Cuántos formatos de fecha distintos puedes identificar? Lista al
menos 3 con ejemplos reales del dataset (valores tal cual aparecen en
la columna `fecha`).

→ **5 formatos**, aunque el script solo reporta 4 patrones. La razón está
en los conteos: tres patrones tienen ~20.300 filas cada uno, pero el
primero tiene 40.644, casi exactamente el doble. Ahí hay dos formatos
mezclados: `dd/MM/yyyy` y `MM/dd/yyyy`, que comparten la misma *forma*
(`\d{2}/\d{2}/\d{4}`) y por eso el regex del script no puede separarlos.

Los 5, con ejemplos reales del dataset:

| Formato | Ejemplo real | Filas |
|---|---|---|
| `yyyy-MM-dd` | `2025-06-14` | 20.284 |
| `yyyy/MM/dd` | `2026/02/13` | 20.293 |
| `dd-MM-yyyy` | `08-06-2026` | 20.279 |
| `dd/MM/yyyy` | `29/10/2025` | ~20.300 (mezclado) |
| `MM/dd/yyyy` | `01/20/2026` | ~20.300 (mezclado) |

Los dos ejemplos de la forma ambigua lo demuestran: `29/10/2025` solo
puede ser `dd/MM` (no hay mes 29) y `01/20/2026` solo puede ser `MM/dd`
(no hay mes 20). Pero cuando el día es <= 12 —como `06/02/2025`— las dos
lecturas son válidas y no hay forma de saber cuál es la correcta sin
metadatos del sistema de origen. Esto va a obligar a tomar una decisión
en Silver 3.2 sobre el orden del `coalesce()`.

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

Ejemplos reales tomados de las muestras del mismo script:

```
04/11/2025    2025/11/20    29/10/2025
08-06-2026    01/20/2026    22-05-2025
2026/02/13    06/02/2025    12-08-2025
```

> Ojo: el script agrupa por *forma*, no por formato real — la primera
> fila junta dos formatos distintos que son indistinguibles cuando el
> día es <= 12.

## 3. Variantes de "Bogotá"

¿Cuántas variantes de "Bogotá" existen en la columna `region`? Lístalas
todas con su conteo.

→ **8 variantes**, que suman 38.603 filas (38% del dataset, la región de
mayor tráfico):

| Variante | Conteo |
|---|---|
| `BOGOTÁ` | 5.017 |
| `Bogota` | 4.956 |
| `bogota` | 4.894 |
| `BTA` | 4.803 |
| `Bta` | 4.796 |
| `BOGOTA` | 4.759 |
| `" Bogotá"` (espacio al inicio) | 4.701 |
| `"Bogotá "` (espacio al final) | 4.677 |

Las dos últimas se ven idénticas en la salida del script; las distingue
solo el espacio. Y `BOGOTÁ` con tilde y `BOGOTA` sin tilde son valores
distintos para Spark, cosa que `upper()` no arregla.

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

> Dos de las filas de Bogotá se ven idénticas en la tabla pero no lo
> son: una tiene espacio al inicio y otra al final.

## 4. Variantes de "app_movil"

¿Cuántas variantes de "app_movil" existen en la columna `canal`?
Lístalas todas con su conteo.

→ **5 variantes**, que suman 35.571 filas (35% del dataset, el canal más
usado):

| Variante | Conteo |
|---|---|
| `App Móvil` | 7.198 |
| `móvil` | 7.158 |
| `app movil` | 7.121 |
| `APP MOVIL` | 7.090 |
| `APP_MOVIL` | 7.004 |

Las diferencias son de mayúsculas, tilde, espacio vs. guion bajo, y una
forma abreviada (`móvil`) que ni siquiera menciona la app.

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

→ **3.959 filas, el 3,90%** de las 101.500. Se descompone en 2.571 nulos
+ 926 negativos + 462 en cero.

Pero ese 3,90% es solo lo que se detecta mirando `total` directamente. El
`max` de la misma tabla es 3.893.000.000, un valor absurdo para un pedido
de e-commerce: son las filas con error de escala (×1000), que pasan
desapercibidas porque son positivas y a primera vista plausibles. La
corrupción real de `total` es mayor que ese 3,90%.

```
=== Estadísticas de 'total' ===
+------------------+-------+----------------+-----+---------+-----+
|min               |max    |mean            |nulos|negativos|ceros|
+------------------+-------+----------------+-----+---------+-----+
|-49989.55707293571|3.893E9|3986873.59396779|2571 |926      |462  |
+------------------+-------+----------------+-----+---------+-----+
```

> Sobre 101.500 filas totales. Fíjate también en el `max` de 3.893
> millones — es la corrupción de escala (x1000) y no la detecta ninguna
> de las tres columnas de arriba.

## 6. Tipo de dato de `vendedor_id`

¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente en
todas las filas?

→ Es **string, y no es consistente**: convive el mismo identificador
escrito de tres formas distintas.

| Forma | Filas | Ejemplo |
|---|---|---|
| Entero puro | 69.592 (68,6%) | `7315` |
| Prefijado `VEN-` | 28.056 (27,6%) | `VEN-3019` |
| Mixto | 3.852 (3,8%) | `v7101` |

Ninguna de las tres es mayoritaria al punto de poder ignorar las otras,
así que en Silver hay que extraer la parte numérica en vez de castear
directo — un `cast("int")` sobre `VEN-3019` daría null y perdería el 31%
de los vendedores.

```
=== Tipos detectados en 'vendedor_id' ===
+-------------+-----+
|tipo_vendedor|count|
+-------------+-----+
|entero       |69592|
|prefijado    |28056|
|mixto        |3852 |
+-------------+-----+
```

Ejemplos reales de cada forma, de las muestras del script:

```
7315        (entero)
VEN-3019    (prefijado)
v7101       (mixto)
```

## 7. Regla de negocio para `total`

¿Qué regla de negocio permite detectar errores en `total`?

→ La regla es **`total = cantidad × precio_unit`**. Es una identidad que
debería cumplirse en toda fila válida, así que cualquier fila donde no se
cumpla está corrupta en al menos una de las tres columnas.

Su valor está en que detecta corrupciones que ninguna inspección de
`total` por sí sola encuentra:

- **Filas con error de escala (×1000):** `total` es positivo y a primera
  vista razonable, pero no cuadra con el producto. Solo la regla las
  delata.
- **Filas con `precio_unit` negativo:** en `PED-025040`, 2 × (−52.100) =
  −104.200, pero `total` dice +104.200. El signo se perdió en el camino,
  lo que revela que `total` no se recalculó a partir de las otras dos
  columnas sino que llegó por su cuenta.

Esa última observación es la que decide la estrategia de Silver: si
`total` fuera un campo derivado y confiable, bastaría con validarlo. Como
llega corrupto de forma independiente, conviene **recalcularlo** desde
`cantidad` y `precio_unit` —que son los datos de origen— en vez de
descartar las filas donde no cuadra.

Evidencia — muestra de filas donde `precio_unit` es negativo pero
`total` es positivo:

```
|pedido_id |cantidad|precio_unit|total   |
|PED-025040|2.0     |-52100.0   |104200.0|
|PED-049796|2.0     |-252400.0  |504800.0|
|PED-087729|5.0     |-115400.0  |577000.0|
```

Estadísticas de las columnas de las que depende la regla:

```
=== Estadísticas de 'precio_unit' ===
|min      |max     |negativos|
|-741400.0|799900.0|641      |

=== Estadísticas de 'cantidad' ===
|min |max|cero_o_negativo|
|-5.0|5.0|622            |
```

## 8. Resumen para ti mismo

Antes de pasar a la Parte 2 (Bronze), resume en 3-4 líneas qué
decisiones de limpieza vas a tener que tomar en Silver a partir de lo
que encontraste aquí.

→ El criterio general fue **descartar solo cuando la fila deja de ser
utilizable, y marcar o corregir en todo lo demás**:

| Problema | Decisión en Silver |
|---|---|
| 1.500 duplicados exactos | Eliminar con `dropDuplicates()` sobre las 14 columnas |
| 5 formatos de fecha | Parsear con `coalesce()` de los 5 patrones; decidir el orden ante la ambigüedad `dd/MM` vs `MM/dd` |
| 35 variantes de región | Mapear a 6 canónicos con `upper(trim())` + diccionario de alias, cubriendo las formas sin tilde |
| 20 variantes de canal | Igual, a 4 canónicos en minúscula con guion bajo |
| `total` corrupto | **Recalcular** desde `cantidad × precio_unit` en vez de filtrar; descartar solo si alguno de los dos factores es inválido |
| `vendedor_id` en 3 formatos | Extraer la parte numérica con `regexp_extract`, no castear directo |
| Emails rotos | **No eliminar**: marcarlos con una columna booleana `email_valido` |

La distinción importante es entre *dato inutilizable* y *dato imperfecto*.
Una fila con `precio_unit` negativo no permite calcular la venta, así que
se descarta. Un email mal escrito no invalida el pedido: la venta ocurrió
y el monto es correcto, así que borrarla perdería una transacción real por
un problema en un campo secundario. Por eso se marca en vez de descartar,
y quien consuma Silver decide si le importa.

---

## Anexo — validación de email

```
=== Validación de 'email_cliente' ===
Emails nulos: 144
Emails con formato inválido (no nulos): 1,175
```

Patrón usado: `^[\w\.\-\+]+@[\w\-]+\.[a-zA-Z]{2,}$`

## Anexo — nulos por columna

```
=== Nulos por columna ===
  total                   2,571  (2.53%)
  pedido_id                 208  (0.20%)
  email_cliente             144  (0.14%)
  fecha                       0  (0.00%)
  categoria                   0  (0.00%)
  producto                    0  (0.00%)
  cantidad                    0  (0.00%)
  precio_unit                 0  (0.00%)
  metodo_pago                 0  (0.00%)
  devuelto                    0  (0.00%)
  calificacion                0  (0.00%)
  region                      0  (0.00%)
  canal                       0  (0.00%)
  vendedor_id                 0  (0.00%)
```
