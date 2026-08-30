# Bitacora de delegacion - Lab 2a

| Tarea | Se delego | Herramienta | Nota |
| :--- | :---: | :--- | :--- |
| Preparar carpeta de entrega | No | - | Se organizó la estructura de directorios bajo el usuario `samedinac` con scripts, configuraciones y evidencias del laboratorio. |
| Levantar infraestructura Kafka | No | - | Se ejecutó Docker Compose con KRaft y se validó el clúster y el topic `pedidos-ventas`. |
| Implementar productor | No | - | Se completó el script del productor de Kafka y el envío sincronizado por lotes con claves por región. |
| Implementar consumidor at-least-once | No | - | Se configuró el `KafkaConsumer` sin auto-commit, control manual de offsets post-merge y la lógica de integración con Delta Lake. |
| Ejecutar ingesta completa | No | - | Se procesaron los flujos de mensajes en tiempo real desde Kafka hacia la capa Bronze. |
| Ejecutar prueba de idempotencia | No | - | Se realizó la simulación de caída abrupta mediante interrupción manual (`Ctrl+C`), conteo de registros y reinicio para probar la no duplicación. |
| Sintaxis de `kafka-python` / PySpark (dudas puntuales) | Sí | Gemini | No me se la sintaxis de estas tecnologías, entonces las consulte con la IA. |
| Generador de datos sintéticos (ya dado) | N/A | - | No hay decisión de diseño ahí — ya viene resuelto. |
| Boilerplate del `docker-compose.yml` (ya dado) | N/A | - | Infraestructura estándar de KRaft — ya viene resuelto, no lo modifiques. |
| El `MERGE` Delta de `merge_a_bronze()` (ya dado) | N/A | - | Ya lo construiste tú mismo en el Lab 1b; aquí solo se reutiliza adaptado a un mensaje. |
| Decidir `key=region` y justificarla | No | - | Es la decisión de diseño central del productor. |
| `enable_auto_commit=False` + coreografía commit-después-del-MERGE | No | - | Es el objetivo 3 de la sesión — si un agente te lo resuelve, no vas a poder explicar la prueba de idempotencia. |
| Ejecutar y documentar la prueba de idempotencia | No | - | Si no la corriste tú, no tienes evidencia real que citar. |
| `kafka_design.md` (las 5 preguntas + Parte 0) | No | - | Es el entregable central del lab. |
| Decidir el número de particiones al crear el topic | No | - | Conecta directo con la regla "N particiones = N consumidores máximos activos" (Pregunta 3). |