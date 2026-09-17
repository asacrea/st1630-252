# Lab 2b - Spark Structured Streaming

**Estudiante:** Sebastian Andres Medina Cabezas
**Fecha de ejecucion:** 13/09/2026
**Parte opcional Kinesis (Parte 4):** Hecho 


## Ejecucion 

Con el Kafka del Lab 2a corriendo y el topic `pedidos-ventas` con datos:

El pipeline lee el JSON desde Kafka, agrega ventas totales y cantidad de
pedidos por region en ventanas de 5 minutos (watermark de 10 minutos), y
escribe los resultados mediante `foreachBatch` + `MERGE` en Delta Lake,
con llave idempotente `(window_start, window_end, region)`.

## Resultado verificado

- Kafka quedo activo y el pipeline leyo el topic `pedidos-ventas`.
- El pipeline agrega por ventana de 5 min y region, y escribe a Delta con
  MERGE idempotente por `(window_start, window_end, region)`.
- Reprocesar un micro-batch no duplica filas: el MERGE actualiza en sitio.
- Parte 4: el stream `pedidos-ventas-kinesis` se creo con 2 shards; el
  productor envio los pedidos (visibles en el Data viewer con TRIM_HORIZON,
  repartidos entre los 2 shards por `PartitionKey=region`); el pipeline
  Qubole leyo de Kinesis y escribio los agregados a Delta.
- Al finalizar se elimino el stream con `aws kinesis delete-stream` para
  no consumir creditos (paso 4.6).


## Documentos relacionados

- Respuestas y decisiones de diseno: `streaming_design.md`
- Declaracion de uso de IA: `bitacora_delegacion.md`
