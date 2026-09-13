# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** ejecución de `04_athena_benchmark.py`
**Estudiante:** Emmanuel Felipe Cortes Rincon - Sara Hurtado Metaute - Juan Jose Osorio - Mariana Sanchez

## Resultados crudos

| Query | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---|---|---|
| 5.1 Top 5 regiones (Gold Parquet, Z-ordered) | 471 | 1.94 | 29,948 |
| 5.2 Misma query (CSV sin particionar) | 731 | 1.37 | 773,897 |

## QueryExecutionId

| Query | QueryExecutionId |
|---|---|
| 5.1 Top 5 regiones (Gold Parquet, Z-ordered) | `d3ced93f-5b78-4b1c-85b8-0e718b355abb` |
| 5.2 Misma query (CSV sin particionar) | `8beb8a45-8443-47bb-9c5f-22567e691154` |

## Ratio de bytes escaneados

**CSV / Parquet = 25.84x**

Interpretacion: el ratio fue mayor al ~9x teorico porque esta prueba
compara una tabla Gold agregada en Parquet contra una muestra CSV de
detalle. Athena pudo leer columnas especificas en Parquet, mientras que
en CSV escaneo el archivo completo de 10,000 filas.
