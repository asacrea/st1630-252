# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** ejecución de `04_athena_benchmark.py`

## Resultados crudos

| Query | Query ID | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---|---:|---:|---:|
| 5.1 Top 5 regiones (Gold Parquet, Z-ordered) | `4fe1f22a-2dcd-46fd-be1f-7fa455c09df3` | 2011 | 2.55 | 29,984 |
| 5.2 Misma query (CSV sin particionar) | `a8e17bbe-6c25-4c49-8579-1fff8953d82d` | 583 | 1.48 | 776,069 |


## Ratio de bytes escaneados

**CSV / Parquet = 25.88x**

> Usa este resultado para completar la Pregunta 5 de
> `pipeline_analysis.md`: ¿coincide con el orden de magnitud teórico
> aproximado de 9x visto en el slide de S4? Si no coincide, analiza el
> tamaño de la muestra, el efecto del formato columnar, la selectividad
> de la consulta y el Z-ordering.
