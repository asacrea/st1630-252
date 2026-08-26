# Bitácora de delegación — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 23/08/2026
**Agentes usados:** Gemini (sesión interactiva) y Cursor/Grok (etapas iniciales)

**Equipo:** Sebastián Andrés Medina Cabezas

**Declarante de esta bitácora:** Sebastián Andrés Medina Cabezas — samedinac@eafit.edu.co

> Esta bitácora declara **mis** delegaciones en la sesión de trabajo que
> produjo esta entrega.
> Conforme a `docs/politica-ia.md`. Declaro abajo toda delegación,
> incluidas las parciales.

## Tabla de delegación

| Tarea | ¿Delegado? | Detalle |
|---|---|---|
| Inspección del `_delta_log` (Bronze) | **Sí** | El agente (Gemini) me ayudó a interpretar el JSON multilínea del log de Delta Lake y a extraer la información de `commitInfo`, `metaData` y `add` esquivando los errores de parseo de Python en la terminal. |
| Ejecución de scripts en EMR por SSH | **Parcial** | Recibí asistencia de Cursor para estructurar los comandos `spark-submit` con los paquetes correctos de Delta Lake y solucionar redirecciones erróneas en la terminal. |
| Código de los bloques `# TODO` de `03_gold.py` | **Parcial** | El diseño base de las agregaciones y el KPI 3 fue estructurado previamente. Gemini me proporcionó la sintaxis exacta de Spark SQL para el `OPTIMIZE ... ZORDER BY` y la creación de tablas en el catálogo. |
| Troubleshooting del Glue Catalog (`SCHEMA_NOT_FOUND` y `TABLE_NOT_FOUND`) | **Sí** | El agente diagnosticó que la base de datos `default` no existía en Glue. Me proporcionó los comandos de AWS CLI para crearla y las sentencias DDL para registrar las tablas manualmente desde la consola de Athena. |
| Preparación del Benchmark (CSV de 10k filas) | **Sí** | El agente escribió el script auxiliar `crear_benchmark.py` para exportar la muestra de Silver a un CSV sin particionar y registrar la tabla externa en Glue. |
| **Respuestas de `pipeline_analysis.md` (5 preguntas)** | **Parcial** | El razonamiento y los datos base son míos (como los 3,959 registros inválidos o el ratio de 25.91x obtenido en mi máquina local). El agente redactó las justificaciones a partir de mis ideas para poder ser más técnico y poder ser más detallado con las explicaciones, lo cual me sirve de aprendizaje para saber como se habla tecnicamente en este ecosistema |


## Nota sobre el método de trabajo

Trabajé con asistentes de IA en un esquema híbrido: utilicé parcialmente Cursor y Grok para la resolución inicial de la capa Silver y el manejo del clúster EMR, y posteriormente interactué con Gemini para destrabar la capa Gold, configurar el catálogo de AWS Glue/Athena y analizar los resultados del benchmark.

## Declaración

Puedo explicar cada línea de los scripts entregados, los comandos ejecutados en la terminal SSH, el propósito de cada script dentro de la arquitectura Medallion y los resultados obtenidos en el benchmark. Comprendo físicamente por qué una operación dispara un *Shuffle* en Spark y cómo el ordenamiento en disco afecta los costos de lectura en Athena.

**Firma:** Sebastián Andrés Medina Cabezas · samedinac@eafit.edu.co · 2026-08-24