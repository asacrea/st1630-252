# Bitácora de delegación · Lab 4a

**Equipo:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio y Mariana Sanchez


| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| `@daily` y `catchup=False` | No | Se mantuvieron las configuraciones de la plantilla y se ajustó la ejecución del DAG. |
| Idempotencia de `ingest_bronze` | No | Seguimos las indicaciones del README para evitar duplicados en Bronze. |
| Idempotencia de `transform_silver` | Parcial | La IA nos ayudó con la implementación del `MERGE`, pero nosotros definimos la estrategia y verificamos que no se generaran duplicados. |
| Checks de `validate_bronze` y umbral de nulos | No | Seguimos los criterios de validación del README y definimos cuándo debía detenerse el pipeline. |
| Casts, filtro de devueltos y agregaciones Gold | No | Realizamos las transformaciones siguiendo los requisitos del laboratorio. |
| Dataset, entorno, pruebas y evidencias | Parcial | La IA nos ayudó con la generación del dataset y algunas pruebas, pero nosotros verificamos los resultados. |
| Redacción de `dag_design.md` | No | Respondimos y documentamos las decisiones de acuerdo con los resultados obtenidos. |

