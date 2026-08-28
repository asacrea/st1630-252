# Data Profiling — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S4-S5 · **Fecha de entrega:** _23/08/2026_  
**Estudiantes:**  
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_ / lemorenog@eafit.edu.co

## 1. Duplicados exactos

¿Cuántos duplicados exactos tiene el dataset?

→ **1.500 filas duplicadas exactas, equivalentes al 1,48 % del dataset.**

```
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

¿Cuántos formatos de fecha distintos puedes identificar? Lista al
menos 3 con ejemplos reales del dataset (valores tal cual aparecen en
la columna `fecha`).

→ **El script detecta 4 patrones, pero en la práctica son 5 formatos**

| Patrón | Filas | % | Ejemplos reales del dataset |
|---|---:|---:|---|
| `dd/MM/yyyy` **o** `MM/dd/yyyy` (ambiguo) | 40.644 | 40,04 % | `29/10/2025` (solo puede ser dd/MM), `01/20/2026` (solo puede ser MM/dd), `04/11/2025` y `06/02/2025` (**ambiguos**) |
| `yyyy/MM/dd` | 20.293 | 20,00 % | `2025/11/20`, `2026/02/13` |
| `yyyy-MM-dd` | 20.284 | 19,98 % | *(no aparece en las muestras impresas, pero el conteo lo confirma)* |
| `dd-MM-yyyy` | 20.279 | 19,98 % | `08-06-2026`, `22-05-2025`, `12-08-2025` |

**Hallazgo crítico:** dentro de los 40.644 registros con `/` conviven ambas
convenciones. Los casos con día ≤ 12 (como `04/11/2025`) son
**irrecuperables sin una regla de negocio adicional**: no hay forma
sintáctica de saber si es 4 de noviembre o 11 de abril.

## 3. Variantes de "Bogotá"

¿Cuántas variantes de "Bogotá" existen en la columna `region`? Lístalas
todas con su conteo.

→ **8 variantes, que suman 38.603 filas (38,03 % del dataset).**

| # | Valor tal cual | Conteo | Problema |
|---|---|---:|---|
| 1 | `BOGOTÁ` | 5.017 | mayúsculas |
| 2 | `Bogota` | 4.956 | sin tilde |
| 3 | `bogota` | 4.894 | minúsculas + sin tilde |
| 4 | `BTA` | 4.803 | abreviatura |
| 5 | `Bta` | 4.796 | abreviatura, otro casing |
| 6 | `BOGOTA` | 4.759 | mayúsculas + sin tilde |
| 7 | `␣Bogotá` | 4.701 | **espacio inicial** |
| 8 | `Bogotá` | 4.677 | forma canónica |

Ninguna variante supera el 5 % del total: no existe un valor dominante que
sirva de "verdad", hay que definir el canónico por diccionario.

## 4. Variantes de "app_movil"

¿Cuántas variantes de "app_movil" existen en la columna `canal`?
Lístalas todas con su conteo.

→ **5 variantes, que suman 35.571 filas (35,04 % del dataset).**

| # | Valor tal cual | Conteo | Problema |
|---|---|---:|---|
| 1 | `App Móvil` | 7.198 | espacio + tilde |
| 2 | `móvil` | 7.158 | palabra suelta, sin "app" |
| 3 | `app movil` | 7.121 | espacio, sin tilde |
| 4 | `APP MOVIL` | 7.090 | mayúsculas + espacio |
| 5 | `APP_MOVIL` | 7.004 | mayúsculas + guion bajo |


## 5. `total` <= 0 o nulo

¿Qué porcentaje de filas tiene `total <= 0` o nulo?

→ **3.959 filas = 3,90 % del dataset.**

| Condición | Filas | % sobre 101.500 |
|---|---:|---:|
| `total` nulo | 2.571 | 2,53 % |
| `total` negativo | 926 | 0,91 % |
| `total` = 0 | 462 | 0,46 % |
| **Total afectado** | **3.959** | **3,90 %** |

## 6. Tipo de dato de `vendedor_id`

¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente en
todas las filas?

→ **Tipo: `string`. NO es consistente.** Conviven dos formatos distintos de
identificador en la misma columna:

- **Con prefijo:** `VEN-3019`, `VEN-1755`
- **Numérico puro:** `7315`, `3324`, `6003`, `6807`, `2743`, `7931`, `7101`

La columna tiene **0 nulos (0,00 %)**, así que el problema no es de
completitud sino de **representación**. El riesgo concreto: si `VEN-1755` y
`1755` son el mismo vendedor, cualquier `GROUP BY vendedor_id` lo cuenta
como dos vendedores distintos y parte sus ventas en dos.

## 7. Regla de negocio para `total`

¿Qué regla de negocio permite detectar errores en `total`?

→ **La regla es: `total = cantidad × precio_unit`** 

Es una **dependencia funcional derivada**: `total` no es un dato
independiente, es una columna calculada. Por eso cualquier discrepancia es
un error de datos comprobable sin fuente externa.

## 8. Resumen para ti mismo

Antes de pasar a la Parte 2 (Bronze), resume en 3-4 líneas qué
decisiones de limpieza vas a tener que tomar en Silver a partir de lo
que encontraste aquí. No hace falta que sean las decisiones finales —
es tu plan de partida.

→ **1)** Deduplicar las 1.500 filas exactas **primero**, y luego normalizar
`region` (35 → 5 ciudades + `DESCONOCIDO`) y `canal` (20 → 4) con diccionario
explícito de abreviaturas, no solo `trim`+`lower`; homogeneizar `vendedor_id`.
**2)** Parsear `fecha` con `coalesce` de los 4 patrones y mandar a cuarentena las
~40k con `/` y día ≤ 12: son ambiguas y asumir una convención corrompe el 40 %.
**3)** Usar `total = cantidad × precio_unit` como validación: recalcular los 2.571
nulos, corregir el signo de `precio_unit`, y enviar a tabla de rechazos (no borrar)
los totales inflados ~1.000×.
**4)** Decidir qué hacer con los 208 `pedido_id` nulos y hashear `email_cliente` (PII).