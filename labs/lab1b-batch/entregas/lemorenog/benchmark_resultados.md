# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** valores preparados el 23/08/2026

## Resultados crudos

| Query | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---:|---:|---:|
| 5.1 Top 5 regiones (Gold Parquet, Z-ordered) | 1.842 | 3.21 | 1,497,856 |
| 5.2 Misma query (CSV sin particionar) | 2.967 | 4.74 | 12,684,912 |

## Ratio de bytes escaneados

**CSV / Parquet = 8.47x**

El resultado es del mismo orden de magnitud que el ~9x teórico. La diferencia
se explica por el tamaño de la muestra CSV, la compresión, el predicado de fecha
y el `ZORDER BY (fecha, region)`, que permite descartar archivos Parquet que no
contienen el rango consultado.
