# Bitácora de delegación — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 31 de agosto de 2026
**Estudiantes:** Hellen Yanes Doria, Sebastian Salazar Henao, Andres
Felipe Velez Alvarez, Samuel Samper Cardona —
Hyanesd@eafit.edu.co, Ssalazarh3@eafit.edu.co, Afveleza@eafit.edu.co,
Ssamperc@eafit.edu.co

## Qué se delegó a IA y qué no

| Tarea | ¿Se delegó? | Detalle |
|---|---|---|
| Sintaxis de `kafka-python`/PySpark (`KafkaProducer`, `KafkaConsumer`, serializers) | No | Implementación completamente manual consultando documentación oficial |
| Generador de datos sintéticos | N/A | Venía resuelto en el script, sin decisión de diseño propia |
| `docker-compose.yml` (boilerplate KRaft) | No | Configuración manual siguiendo documentación de Kafka en KRaft mode |
| El `MERGE` Delta de `merge_a_bronze()` | N/A | Venía dado, reutilizado del Lab 1b sin cambios |
| Decidir `key=region` y justificarla | No — decisión propia | Decisión de diseño tomada por nosotros basada en el análisis de particionamiento y ordenamiento de mensajes |
| `enable_auto_commit=False` + coreografía commit-después-del-MERGE | No — implementación propia | Diseño e implementación manual del flujo de consumidor con commit controlado |
| Ejecutar y documentar la prueba de idempotencia | No — ejecutada por nosotros | Todos los comandos, números (N=109, N'=144, total=distintos=144) y logs fueron generados corriendo el pipeline real en nuestra máquina |
| Decidir el número de particiones al crear el topic | No — decisión propia | Seguimos el valor pedido explícitamente por el enunciado del README (4 particiones) |
| Redacción final de `kafka_design.md` | No | Documento redactado completamente por nosotros con base en nuestra experiencia y resultados |
| Troubleshooting de infraestructura (Docker, PySpark, WSL2, winutils, timeouts de Kafka) | No | Todos los problemas de entorno fueron resueltos manualmente consultando documentación y foros |

## Resumen

El diseño central del lab (decisión de key, coreografía de commit, número de particiones) fue completamente nuestro. Toda la implementación, ejecución, pruebas, troubleshooting y documentación fueron realizadas sin asistencia de IA, basándonos exclusivamente en documentación oficial, materiales del curso y resolución manual de problemas.