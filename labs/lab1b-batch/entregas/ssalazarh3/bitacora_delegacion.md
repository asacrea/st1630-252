# Bitácora de delegación — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6

**Estudiantes:** Sebastian Salazar Henao — ssalazarh3@eafit.edu.co, Andres Felipe Velez Alvarez - afveleza@eafit.edu.co, Samuel Samper Cardona - ssamperc@eafit.edu.co, Hellen Yanes Doria - hyanesd@eafit.edu.co

Este lab sigue `../../docs/politica-ia.md`. A continuación se documenta la resolución del laboratorio. Para este desarrollo **no se delegó ninguna tarea a un asistente de IA**, ni la IA completó ningún componente o parte del trabajo. Todo fue resuelto de forma completamente autónoma por el estudiante.

| Tarea | ¿Se delegó? | Detalle |
|---|---|---|
| Sintaxis de PySpark (groupBy, agg, Window, regexp_extract, rlike) | No | Resuelto de forma autónoma por el estudiante para completar los bloques `# TODO` de `02_silver.py` y `03_gold.py` sin asistencia de IA. |
| Troubleshooting de errores de AWS CLI / EMR / SSH / IAM / Delta Lake | No | Diagnóstico y solución autónoma de problemas de entorno (permisos `.pem`, security groups, key pairs, políticas de IAM en AWS Academy y flags de Delta Lake en `spark-submit`). |
| Boilerplate ya dado en los scripts (imports, rutas, verificaciones) | No aplica | No se tocó — venía resuelto en el repositorio del curso |
| Contenido de `MAPA_REGION` / `MAPA_CANAL` (TODOs 3.3 y 3.4 de Silver) | No | Se completó autónomamente a partir del profiling propio ejecutado en el clúster (`00_profiling.py`), identificando manualmente las variantes reales de "Bogotá" y "app_movil". |
| Lógica y decisiones de los TODOs de `01_bronze.py`, `02_silver.py`, `03_gold.py` (filtros, fórmulas, diseño del KPI 3) | No | Toda la sintaxis, reglas de negocio, recálculo de `total` y diseño del KPI 3 fueron desarrollados 100% por el estudiante. |
| Clasificación NARROW/WIDE de cada bloque transformador | No | Evaluado y completado exclusivamente por el estudiante aplicando el criterio de shuffle/particionamiento. |
| Diseño del KPI 3 (cohortes de clientes por canal) | No | Selección de dimensiones (`canal`, `metodo_pago`) y métricas efectuada totalmente de forma autónoma. |
| `data_profiling.md` (las 8 preguntas) | No | Redactado e interpretado íntegramente por el estudiante a partir del output de `00_profiling.py`. |
| `pipeline_analysis.md` (las 5 preguntas) | No | Redacción, análisis de planes físicos y cálculos auxiliares realizados de manera totalmente autónoma por el estudiante. |
| Interpretación del benchmark Athena (ratio CSV/Parquet) | No | Análisis de resultados (ratio 25.88x) y redacción técnica explicativa realizada exclusivamente por el estudiante. |

## Nota de contexto

Todo el trabajo de esta sesión, incluyendo tanto la resolución de problemas de **configuración de infraestructura** (SSH, key pairs, security groups, credenciales de AWS Academy, Delta Lake) como las decisiones de **diseño de datos** (normalización, recálculo de métricas, clasificación NARROW/WIDE y redacción de los documentos `.md`), fue asumido y completado en su totalidad por el estudiante sin la ayuda ni generación de contenido por parte de ninguna IA.