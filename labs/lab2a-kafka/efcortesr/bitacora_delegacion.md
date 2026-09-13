# Bitacora de delegacion - Lab 2a

| Tarea                                | Se delego | Herramienta | Nota                                                                                                                       |
| ------------------------------------ | --------: | ----------- | -------------------------------------------------------------------------------------------------------------------------- |
| Preparar entorno local               |        No |             | Se creo .venv, se agrego requirements.txt del lab                                                                          |
| Preparar carpeta de entrega          |        No |             | Se creo la estructura efcortesr/ con scripts, datos y documentos de evidencia.                                             |
| Levantar infraestructura Kafka       |        No |             | Se ejecuto Docker Compose local, se corrigio el `CLUSTER_ID` KRaft y se creo el topic requerido.                           |
| Implementar productor                |        no |             | Se completo KafkaProducer, serializacion JSON, `acks="all"`, envio sincronico con `key=region` y resumen region-particion. |
| Implementar consumidor at-least-once |        No |             | Se completo KafkaConsumer, trazabilidad Kafka, MERGE Delta y commit manual posterior al MERGE.                             |
| Ejecutar ingesta completa            |        No |             | Se publicaron 1.000 mensajes y se ingirieron 1.000 filas en Bronze con lag final 0.                                        |
| Ejecutar prueba de idempotencia      |        no |             | Se reproceso controladamente.                                                                                              |
| Completar kafka_design.md            |        Si | Codex       | Se documento con evidencia real de productor, consumidor, lag y Bronze. Se utilizò IA como asistente de redaccion          |
