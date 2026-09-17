# Bitácora de delegación — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

**Agente usado:** Claude Code (Opus 5), sesión del 13/09/2026.

Según `../../../docs/politica-ia.md`.

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Entorno: contenedor de Spark, resolución de los JAR de Delta y Kafka, rutas de Git Bash | Sí | Troubleshooting de entorno, delegable por defecto en la política |
| Diagnóstico del topic vacío (retención de 7 días) | Sí | Troubleshooting de infraestructura |
| Revisión de la Parte 4: desajuste entre `crear_stream_kinesis()` y el conector del README | Sí | Diagnóstico técnico, previo a la decisión |
| Decisión de no hacer la Parte 4 (opcional) | No | Decisión nuestra, tomada después de conocer ese diagnóstico |
| TODO Parte 1 — `crear_stream_kafka()` | Sí | Sintaxis de Structured Streaming, delegable según la rúbrica. El agente lo escribió y nosotros lo revisamos |
| TODO Parte 2 — **tamaño de ventana (5 min) y watermark (10 min)** | No | Decisión nuestra, elegida entre alternativas con sus trade-offs; la justificamos en la Pregunta 1 |
| TODO Parte 2 — sintaxis de `aplicar_ventana()` | Parcial | La escribió el agente con los valores que elegimos y lo auditamos nosotros |
| TODO Parte 3 — `escribir_batch()` | Parcial | La llave `(window_start, window_end, region)` venía especificada en el enunciado; el agente escribió la implementación y la justificación de la llave es nuestra (Pregunta 4) |
| Prueba de humo del MERGE en un topic aparte | Sí | El agente verificó su propia implementación antes de entregárnosla; esos datos no forman parte de nuestra evidencia |
| **Corridas del productor y verificación de que Silver no duplica** | No | Corrimos nosotros las 4 corridas del productor y las verificaciones de Silver |
| Lectura del historial de Delta y del checkpoint para citar evidencia | No | Evidenciamos las corridas en las respuestas de las preguntas |
| Prueba de reinicio del pipeline y de un pedido tardío | No | Evidencia en las preguntas 1 y 6 |
| `streaming_design.md` | No | Entregable central del lab; lo redactamos nosotros con los datos medidos |
