# Lab 2b — Streaming con Spark Structured Streaming (Kafka)

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha de entrega:** 13/09/2026
**Estudiantes:** Hellen Yanes Doria, Sebastian Salazar Henao, Andres Velez Alvarez, Samuel Samper Cardona

## Alcance de esta entrega

- **Partes 1-3 (obligatorias, Kafka): completas.** `crear_stream_kafka()`,
  `aplicar_ventana()` (ventana de 2 minutos, watermark de 3 minutos) y
  `escribir_batch()` (MERGE por `window_start, window_end, region`)
  están implementadas y verificadas corriendo el pipeline completo
  contra el topic `pedidos-ventas` del Lab 2a.
- **Parte 4 (opcional, Kinesis): no realizada.** El ambiente de AWS
  Academy disponible no tiene el servicio Kinesis habilitado
  (`AccessDeniedException` al intentar `kinesis:CreateStream`). No
  afecta la nota base del lab, solo el +10% adicional.

## Verificación de idempotencia (Parte 3)

Corrí `productor_kafka.py` 3 veces (separadas por más de 3 minutos
cada una) mientras `streaming_pipeline.py` corría contra Kafka. La
tabla Delta resultante tiene 18 filas (6 regiones × 3 ventanas:
`20:14-20:16`, `21:20-21:22`, `21:26-21:28`), sin ninguna fila
duplicada por `(window_start, window_end, region)` — evidencia
adjunta como captura de pantalla en el PR.

## Contenido de esta carpeta

- `scripts/streaming_pipeline.py` — pipeline completo con los 3 TODO resueltos.
- `streaming_design.md` — respuestas a las Preguntas 1-6 (Pregunta 7 no aplica).
- `bitacora_delegacion.md` — qué se delegó y qué no.
