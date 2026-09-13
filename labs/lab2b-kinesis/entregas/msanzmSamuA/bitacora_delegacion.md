# Bitácora de delegación — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026  
**Equipo:** Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`), Mateo Sanz Medina (`msanzm@eafit.edu.co`), Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

Este laboratorio sigue las políticas de uso de IA definidas en `docs/politica-ia.md`.

---

## Matriz de Delegación de Tareas

| Tarea del Laboratorio | ¿Se puede delegar? | ¿Fue delegada a IA? | Justificación y Declaración del Equipo |
| :--- | :---: | :---: | :--- |
| **Sintaxis de Structured Streaming** (dudas de API, casting de schema) | Sí | Parcial | Se consultó la sintaxis exacta de `from_json` y la integración del paquete `spark-sql-kafka-0-10` con PySpark. |
| **Scripts dados de Kinesis** (`kinesis_producer.py` y `crear_stream_kinesis()`) | N/A | No | Fueron provistos directamente por la cátedra para fines pedagógicos de comparativa. |
| **Decisión de tamaño de ventana y watermark** | **No** | **No** | **Decisión asumida por el equipo:** Se definió una ventana tumbling de 5 minutos y un watermark de 10 minutos para balancear frescura analítica y tolerancia a latencia de red móvil. |
| **Diseño de la llave del MERGE del sink** | **No** | **No** | **Decisión asumida por el equipo:** Se identificó que la identidad lógica en Silver es la tupla `(window_start, window_end, region)` para consolidar métricas agregadas en vez de eventos individuales. |
| **Verificación de idempotencia tras múltiples corridas** | **No** | **No** | **Ejecutado por el equipo:** Se verificó experimentalmente en el log de Spark y en Delta Lake que las filas existentes se actualicen sin duplicarse. |
| **Elaboración de `streaming_design.md`** | **No** | **No** | **Redacción del equipo:** Justificación conceptual y arquitectónica de las 7 preguntas técnicas evaluadas. |

---

## Declaración de Autoría y Comprensión

Declaramos que comprendemos cabalmente el funcionamiento interno del pipeline de streaming:
1. Cómo opera el motor de micro-batches de Spark Structured Streaming coordinado con `checkpointLocation`.
2. El mecanismo mediante el cual el watermark gestiona la expiración de ventanas y previene el desbordamiento de memoria.
3. La garantía de consistencia matemática del patrón `foreachBatch` + `DeltaTable.merge()` para lograr procesamiento *exactly-once* semántico.
