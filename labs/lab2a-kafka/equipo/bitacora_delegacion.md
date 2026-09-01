# Bitácora de delegación — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 2026-08-31
**Estudiantes:**
Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co · Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
Sebastián Durán Fernández — sduranf@eafit.edu.co · Daniel Arcila Salazar — darcilas1@eafit.edu.co

> **Herramienta utilizada en todas las tareas delegadas de este lab:**
> Claude Code (Opus 5).

## Declaración previa

En este laboratorio se delegó al agente **el código de los TODO, el montaje
del entorno, el troubleshooting y la redacción de los documentos de
evidencia**.

**No se delegó `kafka_design.md`**, que es el entregable central: lo
escribimos nosotros. Sí fue delegada la **recopilación y el ordenamiento de
la evidencia** de nuestras corridas —las salidas de terminal agrupadas por
pregunta— pero el análisis, la estructura y los argumentos del documento son
nuestros. Tampoco fueron delegadas las capturas de Kafka UI
(`datos/kafka_ui_lag_cero.png` y `datos/kafka_ui_particiones.png`).

Las ejecuciones son reales. El clúster, el productor, el consumidor, la
caída provocada con `SIGKILL` y todos los conteos ocurrieron en una máquina
del equipo, y lo que hay en `datos/` es la salida literal de esas corridas.
Los comandos los ejecutó el agente sobre esa máquina; ningún resultado fue
fabricado ni ajustado.

## Tabla de tareas

Las filas siguen el orden de la tabla del README del lab, para poder
compararla directo con lo que el enunciado esperaba.

| Tarea | ¿Delegable según el lab? | ¿Delegado en la práctica? | Detalle |
|---|---|---|---|
| Sintaxis de `kafka-python`/PySpark | Sí | **Sí** | Uso autorizado por el enunciado. |
| Generador de datos sintéticos | N/A (ya dado) | No aplica | Venía resuelto; no se tocó. |
| Boilerplate del `docker-compose.yml` | N/A (ya dado) | No aplica | No se modificó el archivo original. Se agregó un `docker-compose.override.yml` porque el `CLUSTER_ID` del compose no es un UUID válido de KRaft y el broker no arranca — ver `README.md`. |
| El `MERGE` Delta de `merge_a_bronze()` | N/A (ya dado) | No aplica | Venía resuelto; no se tocó. |
| Decidir `key=region` y justificarla | **No** | **No delegado** | La key ya venía fijada por el enunciado. El argumento de la Pregunta 2 —orden por clave, la colisión de hash entre Bogotá y Cali que llevó P0 al 55,6 %, la alternativa y el *salting*— es nuestro, sobre las cifras de nuestra corrida. |
| `enable_auto_commit=False` + coreografía commit-después-del-MERGE | **No** | **Sí — delegado** | El agente escribió el código del TODO 2.1 y el `try/except` del TODO 2.2/2.3. |
| Ejecutar y documentar la prueba de idempotencia | **No** | Ejecución real; **redacción delegada** | La corrida es real: `SIGKILL` al consumidor, conteos antes/después e historial de Delta. El agente ejecutó los comandos y redactó `datos/prueba_idempotencia.md`. La interpretación de esa evidencia en la Pregunta 1 es nuestra. |
| `kafka_design.md` (5 preguntas + Parte 0) | **No** | **No delegado** | Escrito por nosotros. La evidencia citada proviene de nuestras corridas; su recopilación y ordenamiento sí fueron delegados (ver tabla siguiente). |
| Decidir el número de particiones | **No** | **No delegado** | Las 4 particiones las fija el enunciado. El análisis de la Pregunta 3 —el matiz entre "sin asignación" y "ocioso en la práctica", y la ruptura del orden por clave al hacer `--alter`— es nuestro. |

## Tareas adicionales, no previstas en el enunciado

Surgieron durante la ejecución. **Todas fueron delegadas**, y todas son
diagnóstico técnico, configuración de entorno u organización de datos, que
el lab clasifica como delegable.

| Tarea | Justificación |
|---|---|
| Diagnóstico del `CLUSTER_ID` inválido y generación de un UUID válido de KRaft | Troubleshooting de infraestructura: el `docker-compose.yml` del lab no arranca tal cual. Es un fallo del material, no una decisión de diseño. |
| Montaje del entorno del consumidor en un contenedor Linux | PySpark no puede escribir en disco en Windows sin `winutils.exe`; se intentó esa vía y falla con `UnsatisfiedLinkError`. Configuración de herramientas, sin valor de aprendizaje del curso. |
| Diagnóstico del `CommitFailedError` por expulsión del consumer group y ajuste de `max_poll_records` | Diagnóstico técnico sobre un error real de nuestra ejecución (evidencia en `datos/consumidor_expulsion_evidencia.txt`). |
| Diagnóstico de la degradación por *small files* (5 s → 20 s por mensaje) y tuning de Spark/Delta | Diagnóstico de rendimiento sobre mediciones de nuestra corrida. Sin este ajuste el lab no terminaba: los 1.000 mensajes habrían tardado más de 5 horas. No cambia la lógica del MERGE ni la coreografía del commit. |
| Recopilación y ordenamiento de la evidencia por pregunta | Organización de datos que ya eran nuestros; no hay decisión de diseño ni argumentación en agrupar salidas de terminal. Es el material sobre el que escribimos `kafka_design.md`. |
| `scripts/contar_bronze.py` y `scripts/benchmark_sync_vs_async.py` | Utilidades de verificación y el opcional 1.5; no son parte del entregable evaluado por la rúbrica. |
| `README.md` de esta entrega | Documentación del entorno y de las desviaciones respecto al enunciado. |

## Nota sobre el impacto en la rúbrica

Esta declaración afecta los criterios de **Productor** (25 %) y **Consumidor
at-least-once** (30 %): el código de esos TODO lo escribió el agente y no lo
presentamos como propio.

**`kafka_design.md` (25 %) y la prueba de idempotencia sí reflejan trabajo
nuestro**: la ejecución del pipeline, la interpretación de lo que arrojó y
la redacción del documento de diseño. Preferimos declarar con precisión qué
fue delegado y qué no, antes que difuminar la línea en cualquiera de las dos
direcciones.
