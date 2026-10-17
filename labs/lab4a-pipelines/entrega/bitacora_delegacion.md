# Bitácora de delegación — Lab 4a

**Curso:** ST1630-2026-2 · **Semana:** S11 · **Fecha:** 16/10/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Schedule interval (`@daily`) | No | Venía dado en el DAG y lo mantuvimos. La justificación en `dag_design.md` está en la fila de redacción |
| Decisión de usar `catchup=False` | No | Venía dado en el DAG y no lo cambiamos |
| `max_active_runs=1` (fuera de los TODO) | Si | El agente detectó que el backfill corre fechas en paralelo y que dos `transform_silver` simultáneos se pisan en la primera escritura de Silver o chocan en el MERGE. Nos planteó tres alternativas y elegimos esta |
| Estrategia de idempotencia en `ingest_bronze` (TODO 1) | No | La implementamos y el agente la revisó |
| Estrategia de idempotencia en `transform_silver` / MERGE (TODO 3) | No | La implementamos y el agente la revisó |
| Diseño de los checks de `validate_bronze` (TODO 2) | No | Los implementamos nosotros y el agente revisó |
| Threshold del check de nulos (5%) | No | Venía dado (`UMBRAL_NULOS`) y no lo cambiamos. El análisis de si tiene sentido está en el borrador de `dag_design.md` |
| Casts, filtro de devueltos y `anio_mes` (TODO 2 de `transform_silver`) | Sí | Sintaxis de PySpark, delegable |
| Agregaciones de `build_gold` (TODO 4) | Sí | Las implementó el agente según las pistas del enunciado. También añadió una guarda para cuando Silver aún no existe (la corrida programada que dispara el scheduler al activar el DAG) |
| Capturas de la UI (Grid, Graph, log de `validate_bronze`) | No | Las tomamos nosotros |
| Redacción de `dag_design.md` | Parcial | Redactamos las ideas principales para cada pregunta y el agente pulió. |
