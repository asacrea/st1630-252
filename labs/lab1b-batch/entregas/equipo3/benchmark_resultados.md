# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** ejecución de `04_athena_benchmark.py`

## Resultados crudos

| Query | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---|---|---|
| 5.1 Top 5 regiones (Gold Parquet, Z-ordered) | 2051 | 3.19 | 34,535 |
| 5.2 Misma query (CSV sin particionar) | 1040 | 1.52 | 1,549,198 |

## Ratio de bytes escaneados

**CSV / Parquet = 44.86x**

> Completa la Pregunta 5 de `pipeline_analysis.md` con este número:
> ¿coincide con el orden de magnitud teórico (~9x) visto en el slide
> de S4? Si no coincide, ¿a qué se lo atribuyes -- tamaño de la
> muestra, efecto del Z-ordering, selectividad de la query?
