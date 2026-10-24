# bitacora_delegacion.md — Lab 4a

**Estudiante:** Sebastian Andres Medina Cabezas - samedinac@eafit.edu.co

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| `@daily` y `catchup=False` | No | Se mantuvieron las configuraciones de la plantilla y se ajustó la ejecución del DAG. |
| Idempotencia de `ingest_bronze` | No | Segui las indicaciones del README para evitar duplicados en Bronze. |
| Idempotencia de `transform_silver` | Parcial | La IA me ayudó con la implementación del `MERGE`, pero yo defini la estrategia y verifique que no se generaran duplicados. |
| Checks de `validate_bronze` y umbral de nulos | No | Segui los criterios de validación del README y defini cuándo debía detenerse el pipeline. |
| Casts, filtro de devueltos y agregaciones Gold | No | Realize las transformaciones siguiendo los requisitos del laboratorio. |
| Dataset, entorno, pruebas y evidencias | No | El dataset se gener con el script dejado por default y los resultados los evidencie manualmente. |
| Redacción de `dag_design.md` | No | Respondi y documente las decisiones de acuerdo con los resultados obtenidos. |
