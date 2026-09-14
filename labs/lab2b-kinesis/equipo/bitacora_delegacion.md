# Bitácora de delegación — Lab 2b

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 2026-09-13
**Estudiantes:**
Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co · Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
Sebastián Durán Fernández — sduranf@eafit.edu.co · Daniel Arcila Salazar — darcilas1@eafit.edu.co

> **Herramienta utilizada en todas las tareas delegadas de este lab:**
> Claude Code (Opus 5).

## Declaración previa

En este laboratorio se delegó al agente **el código de los tres TODO, el montaje del entorno,
el troubleshooting de la Parte 4 y la redacción de todos los documentos**, incluido
`streaming_design.md`.

**Lo que no se delegó son las decisiones.** Nosotros elegimos la ventana y el watermark,
explicamos por qué la llave del `MERGE` no puede ser `pedido_id` y por qué `append` duplica,
predijimos el resultado de las pruebas de reinicio antes de correrlas, elegimos qué
diferencias y criterios pesaban más en las Preguntas 2 y 7, y decidimos mantener Kafka en la
7(c). El agente trabajó con preguntas guiadas: nos mostraba la evidencia de nuestras corridas,
nosotros respondíamos en una o dos frases y él redactaba el párrafo a partir de esa respuesta,
agregando explicación técnica y cifras. Revisamos cada respuesta antes de guardarla.

**`streaming_design.md` está marcado como no delegable en el README y lo declaramos como
parcialmente delegado**: las respuestas centrales son nuestras, la redacción no. Preferimos
declararlo así antes que presentarlo como escrito a mano.

Las ejecuciones son reales y ocurrieron en una máquina del equipo. Nada en `datos/` fue
fabricado ni ajustado. Parte de los comandos los corrimos nosotros en la terminal y parte los
corrió el agente; la tabla de abajo dice cuál.

## Tabla de tareas

Las filas siguen el orden de la tabla del README del lab.

| Tarea | ¿Delegable según el lab? | ¿Delegado en la práctica? | Detalle |
|---|---|---|---|
| Sintaxis de Structured Streaming | Sí | **Sí** | El agente escribió `crear_stream_kafka()` (Parte 1), el cuerpo de `aplicar_ventana()` (Parte 2) y `escribir_batch()` (Parte 3), siguiendo los pasos de cada TODO. |
| `kinesis_producer.py` y `crear_stream_kinesis()` (ya dados) | N/A | **Modificado por el agente** | El productor no se tocó. En `crear_stream_kinesis()` el agente agregó `kinesis.endpointUrl` (solo si existe `KINESIS_ENDPOINT`) y la carga del JAR del conector, para poder correr contra LocalStack. Ver "Tareas adicionales". |
| Decidir el tamaño de ventana y el watermark | **No** | **Decisión no delegada; justificación con apoyo** | Elegimos 5 minutos de ventana ("cada corrida del productor cabe entera en una ventana") y 10 de watermark ("tolerar pedidos atrasados sin guardar tanto estado"). Los valores coinciden con el ejemplo del TODO, que el agente nos mostró junto con el trade-off. La cuantificación (~18 entradas de estado) y la redacción de la Pregunta 1 son del agente. |
| Diseñar la llave del `MERGE` del sink | **No** | **Explicación nuestra; código delegado** | La llave `(window_start, window_end, region)` ya venía escrita en el TODO. Nosotros explicamos por qué `append` duplica la ventana, por qué en Silver no existe `pedido_id` y que el reproceso depende del estado del checkpoint. El código lo escribió el agente. |
| Verificar que el sink no duplica tras varias corridas | **No** | **Ejecución real compartida; redacción delegada** | Nosotros levantamos Kafka, construimos la imagen, arrancamos el pipeline y corrimos el productor 4 veces y la Prueba 2 (reinicio sin checkpoint, con nuestra predicción antes). La Prueba 1 (reinicio con checkpoint) la corrimos nosotros y el agente casi al mismo tiempo, lo que dejó dos caídas seguidas. El agente leyó la tabla, el historial y el checkpoint después de cada corrida y redactó `datos/verificacion_sink.md`. |
| `streaming_design.md` | **No** | **Parcialmente delegado** | Respuestas y decisiones nuestras, dadas pregunta por pregunta; redacción, explicación técnica y organización de la evidencia del agente. Ver "Declaración previa". |

## Tareas adicionales, no previstas en el enunciado

**Todas fueron delegadas.** Son configuración de entorno, troubleshooting y documentación.

| Tarea | Justificación |
|---|---|
| Traer los cambios del repo del profesor (`git merge upstream/master -X ours`) | Hubo un conflicto en `lab1b-batch/scripts/01_bronze.py` (nuestro TODO resuelto contra el comentario actualizado del profesor). Operación de git, sin decisión de diseño. El commit lo hicimos nosotros. |
| Reutilizar el entorno Docker del Lab 2a (Kafka con el override de `CLUSTER_ID` y la imagen `st1630-lab2a-spark`) | En la máquina no había PySpark, y Python 3.13 con Java 21 no sirve para Spark 3.5. Mismo motivo que en el Lab 2a. |
| Diagnóstico de `kinesis:CreateStream` denegado en AWS Academy | Las credenciales las renovamos nosotros; el agente verificó el acceso con llamadas de solo lectura y explicó el `AccessDeniedException`, que ocurrió en `us-east-1` y `us-west-2`. |
| Montar LocalStack 3.8 como sustituto de Kinesis | Alternativa de entorno para poder hacer la Parte 4. Nosotros corrimos los comandos; el agente los propuso. |
| Elegir, descargar e inspeccionar el conector de Kinesis para Spark | El paquete de Qubole que cita el README es para Spark 3.0 y no corresponde a `format("aws-kinesis")`. El agente identificó el conector de AWS Labs, lo descargó (con nuestra autorización) desde el bucket oficial y verificó sus opciones. |
| Diagnóstico de `Invalid endpoint url received. Cannot parse region` y alias de red `kinesis.us-east-1.localstack` | Rareza del conector con endpoints propios, comprobada llamando su función desde una JVM aparte. Troubleshooting sin valor de diseño. |
| `scripts/leer_ventanas.py` | Utilidad de verificación; no es parte del entregable evaluado. |
| `datos/verificacion_sink.md` y `datos/verificacion_kinesis.md` | Redacción de la evidencia de nuestras corridas, con salidas literales. Reemplaza la captura del Data Viewer por la salida de `get-records`, porque LocalStack no tiene consola. |
| `README.md` de esta entrega y esta bitácora | Documentación del entorno, de las desviaciones y de la delegación. |

## Nota sobre el impacto en la rúbrica

- **Lectura Kafka + ventana (30 %) y sink idempotente (30 %):** el código de los TODO lo
  escribió el agente. Las decisiones de ventana y watermark, la explicación de la llave y la
  ejecución de la verificación son nuestras.
- **`streaming_design.md` (30 %):** declarado como parcialmente delegado. Las respuestas de
  fondo son nuestras y podemos defenderlas; la redacción no.
- **Parte 4 — Kinesis (+10 %):** hecha contra LocalStack, no contra AWS, porque AWS Academy no
  permite crear el stream. No hay captura del Data Viewer ni recursos de AWS que limpiar; se
  reemplazó por la salida de `get-records` y se limpian los contenedores locales al terminar.

Preferimos declarar con precisión qué fue delegado y qué no, antes que difuminar la línea en
cualquiera de las dos direcciones.
