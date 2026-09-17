## Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Sintaxis de Structured Streaming (dudas puntuales) | Sí | Bajo valor de aprendizaje memorizar sintaxis |
| `kinesis_producer.py` y `crear_stream_kinesis()` (ya dados) | N/A | El objetivo de la Parte 4 es comparar, no reimplementar una decisión ya tomada en el Lab 2a |
| Decidir el tamaño de ventana y el watermark | **No** | Es la decisión de diseño central de la Parte 2 |
| Diseñar la llave del `MERGE` del sink | **No** | Objetivo central de la Parte 3 — conecta directo con la idempotencia del Lab 2a |
| Verificar que el sink no duplica tras varias corridas del productor | **No** | Si no la corriste tú, no tienes evidencia real que citar |
| `streaming_design.md` | **No** | Es el entregable central del lab |