# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** ejecución de `04_athena_benchmark.py`

**Estudiantes:**
- Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co
- Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
- Sebastián Durán Fernández — sduranf@eafit.edu.co
- Daniel Arcila Salazar — darcilas1@eafit.edu.co

## Resultados crudos

| Query | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---|---|---|
| 5.1 Top 5 regiones (Gold Parquet, Z-ordered) | 2093 | 4.38 | 29,948 |
| 5.2 Misma query (CSV sin particionar) | 528 | 1.33 | 775,264 |

## Ratio de bytes escaneados

**CSV / Parquet = 25.89x**

> Completa la Pregunta 5 de `pipeline_analysis.md` con este número:
> ¿coincide con el orden de magnitud teórico (~9x) visto en el slide
> de S4? Si no coincide, ¿a qué se lo atribuyes -- tamaño de la
> muestra, efecto del Z-ordering, selectividad de la query?
