# Bitácora de delegación — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 31/08/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

**Agente usado:** Claude Code (Opus), sesión del 31/08/2026.

Según `../../../docs/politica-ia.md`. Declaramos todo lo que se delegó,
incluido lo que la rúbrica del lab marca como no delegable.

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Diagnóstico del `CLUSTER_ID` inválido de KRaft y el `docker-compose.override.yml` | Sí | Troubleshooting de infraestructura; el error no tiene valor pedagógico y está fuera del alcance del lab (bug del compose del curso) |
| Diagnóstico de PySpark con Python 3.13/3.12 y de `NativeIO$Windows.access0` en Windows | Sí | Troubleshooting de instalación de entorno — explícitamente delegable por defecto en la política |
| Montaje del contenedor `apache/spark:3.5.3-python3` para correr el consumidor en Linux | Sí | Decisión de entorno, no de diseño del pipeline; los scripts no se modificaron |
| Sintaxis de `kafka-python` (serializers, `future.get`, `commit`) | Sí | Bajo valor de aprendizaje memorizar la firma de la librería; la rúbrica lo declara delegable |
| Comandos de la Parte 0 (crear topic, listar, `--describe`, mensajes de prueba con y sin key) | No | Ejecución de los comandos y captura de las salidas; interpretación de cada observación en `kafka_design.md` |
| **TODO 1.1 / 1.3 / 1.4 del productor** (config del `KafkaProducer`, envío síncrono con `key=region`, conteo región→partición) | Parcial | Lo escribió el agente; nosotros lo revisamos línea por línea antes de correrlo. La *decisión* de `key=region` venía dada por el enunciado del script; su justificación es nuestra (Pregunta 2) |
| **TODO 2.1 y 2.2/2.3 del consumidor** (`enable_auto_commit=False` y el commit después del MERGE) | Parcial | Lo escribió el agente y nosotros lo revisamos. |
| Ejecución de la prueba de idempotencia y captura de su evidencia | No | Ejecución manual del consumer, se detiene antes del commit y conteo directo en Delta Lake demostrando la lectura de por qué `N = N'` |
| Decidir el número de particiones del topic (4) | No | Lo fijamos nosotros siguiendo la regla “N particiones = N consumidores máximos activos” (Pregunta 3) |

