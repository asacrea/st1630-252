## Bitácora de delegación

**Entregable 1 — TiendaCol en tiempo real.** Herramienta usada: Claude (Claude Code).

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Decisiones de arquitectura (Kafka, Spark, Delta Lake, DynamoDB, Airflow, AWS) | No | Son decisiones de diseño centrales; las toma el equipo |
| Redacción de los requisitos no funcionales, del ADR-1 y del ADR-2 | Parcial | Borrador del agente a partir de decisiones ya adoptadas por el equipo, con valores de configuración propuestos (topics, particiones, *watermarks*) que el equipo validó |
| Que la capa Silver la produzca solo el batch horario | Parcial | Recomendación del agente, aprobada por el equipo: un único escritor por tabla evita conflictos de escritura concurrente en Delta Lake; la deduplicación por `event_id` es completa solo en el batch, que ve toda la hora de Bronze; y el batch es idempotente y reprocesable desde Bronze |
| Revisión del diagrama frente a los ADR | Sí | Verificación de consistencia de bajo valor de aprendizaje; detectó el doble responsable de Silver, el canal indefinido del RF-09 y un `.drawio` desactualizado |
| Reducción de la extensión de los ADR para cumplir el límite de 15 páginas | Sí | Tarea mecánica de edición |
| Generación del PDF | Sí | Tarea mecánica de formato |

<!-- Pendiente: agregar las filas de P1, P3 y P4 con lo que cada uno delegó, antes de abrir el PR. -->
