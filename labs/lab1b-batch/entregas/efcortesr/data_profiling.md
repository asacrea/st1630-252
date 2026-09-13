# Data Profiling - Lab 1b

**Curso:** ST1630-2026-2 - **Semana:** S5-S6 - **Fecha:** 2026-08-20  
**Estudiante:** Emmanuel Felipe Cortes Rincon - Sara Hurtado Metaute - Juan Jose Osorio - Mariana Sanchez

Evidencia usada:

- Log completo de EMR: `evidencia/00_profiling_output.txt`
- Resumen limpio auxiliar: `evidencia/profiling_resumen_limpio.md`
- Step EMR ejecutado: `s-04666612ZIUH92BNLFD6`
- Cluster EMR: `j-09419123FVA3WGNZBANI`

## 1. Duplicados exactos

-> El dataset tiene 1,500 duplicados exactos sobre 101,500 filas. Eso equivale a 1.48% del total y confirma que Silver debe deduplicar antes de aplicar reglas de calidad.

Evidencia:

```text
Filas totales: 101,500
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

-> Identifique 4 patrones de texto en la columna `fecha`. Uno de ellos es ambiguo porque valores con formato `dd/MM/yyyy` y `MM/dd/yyyy` pueden tener la misma forma cuando dia y mes son menores o iguales a 12. Por eso en Silver se debe probar varios formatos y usar un orden de parseo consistente.

Evidencia:

```text
dd/MM/yyyy o MM/dd/yyyy (ambiguo): 40,644
  ejemplos: 09/06/2025, 03/19/2025, 18/07/2025
yyyy/MM/dd: 20,293
  ejemplos: 2025/01/23, 2025/07/16, 2026/03/21
yyyy-MM-dd: 20,284
  ejemplos: 2025-01-10, 2026-05-23, 2026-04-27
dd-MM-yyyy: 20,279
  ejemplos: 01-06-2025, 21-06-2026, 08-06-2026
```

## 3. Variantes de "Bogota"

-> En la evidencia aparecen 8 variantes que corresponden a Bogota si se incluyen los alias `BTA` y `Bta`. El criterio es agrupar diferencias de mayusculas/minusculas, tildes, espacios laterales y abreviaturas conocidas bajo un valor canonico unico.

Evidencia:

```text
BOGOTA/BOGOTA con tilde, espacios y alias:
BOGOTA con tilde en mayuscula: 5,017
Bogota  : 4,956
bogota: 4,894
BTA: 4,803
Bta: 4,796
BOGOTA: 4,759
 Bogota con tilde: 4,701
Bogota con tilde: 4,677
```

Ver tambien la tabla completa de 35 variantes en `evidencia/profiling_resumen_limpio.md`.

## 4. Variantes de "app_movil"

-> En la evidencia aparecen 5 variantes del canal `app_movil`: `App Movil`, `movil`, `app movil`, `APP MOVIL` y `APP_MOVIL` (con diferencias de tilde, mayusculas y separadores). Todas deben mapearse al valor canonico `app_movil`.

Evidencia:

```text
App Movil con tilde: 7,198
movil con tilde: 7,158
app movil: 7,121
APP MOVIL: 7,090
APP_MOVIL: 7,004
```

Ver tambien la tabla completa de 20 variantes en `evidencia/profiling_resumen_limpio.md`.

## 5. `total` <= 0 o nulo

-> Hay 3,959 filas con `total <= 0` o `total` nulo sobre 101,500 filas. Eso representa 3.90% del dataset e incluye 2,571 filas donde `total` viene nulo.

Evidencia:

```text
Filas totales: 101,500
total nulo: 2,571
total <= 0 o nulo: 3,959 (3.90%)
```

## 6. Tipo de dato de `vendedor_id`

-> `vendedor_id` no es consistente como identificador: 69,592 filas vienen como entero puro, 28,056 con prefijo `VEN-` y 3,852 en otros formatos mixtos. Para Silver conviene extraer solo la parte numerica y almacenar un identificador homogeno.

Evidencia:

```text
vendedor_id entero: 69,592
vendedor_id prefijado VEN-: 28,056
vendedor_id mixto: 3,852
```

## 7. Regla de negocio para `total`

-> La regla de negocio es `total_correcto = cantidad * precio_unit`. El campo `total` del raw no debe ser fuente confiable porque puede venir nulo, menor o igual a cero, o no cuadrar con la multiplicacion esperada; por eso Silver debe recalcularlo despues de validar `cantidad` y `precio_unit`.

Evidencia relevante:

```text
Regla esperada: total_correcto = cantidad * precio_unit
total nulo: 2,571
precio_unit negativos: 641
cantidad <= 0: 622
```

## 8. Resumen para Silver

-> En Silver voy a deduplicar las filas exactas, parsear `fecha` con los formatos detectados y normalizar `region` y `canal` a valores canonicos. Despues voy a convertir `cantidad` y `precio_unit` a numericos, filtrar valores no positivos y recalcular `total_silver` con la regla `cantidad * precio_unit`. Finalmente voy a limpiar `vendedor_id` extrayendo la parte numerica y marcar `email_valido` con una validacion regex sin eliminar pedidos solo por email invalido.

Pistas basadas en la evidencia:

- Parsear fechas con varios formatos y resolver el caso ambiguo.
- Normalizar `region` y `canal` a valores canonicos.
- Recalcular `total_silver` desde `cantidad * precio_unit` despues de validar positivos.
- Extraer la parte numerica de `vendedor_id` y marcar `email_valido` sin eliminar filas solo por email invalido.
