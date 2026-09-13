# Entregable Lab 2b — Streaming con Spark Structured Streaming (Kafka & Kinesis)

**Curso:** ST1630-2026-2 · **Semana:** S7 · **Fecha:** 13/09/2026  
**Equipo:** Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`), Mateo Sanz Medina (`msanzm@eafit.edu.co`), Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

---

## 1. Estructura de la Entrega

```
labs/lab2b-kinesis/entregas/msanzmSamuA/
├── scripts/
│   ├── streaming_pipeline.py     # Pipeline Structured Streaming completo (Kafka/Kinesis + Window + Delta MERGE)
│   └── kinesis_producer.py       # Productor de eventos sintéticos para Amazon Kinesis Data Streams
├── streaming_design.md           # Respuestas a las Preguntas 1 a 6 + Pregunta 7 (Kinesis)
├── bitacora_delegacion.md        # Bitácora de cumplimiento de política de IA (docs/politica-ia.md)
└── README.md                     # Este archivo con instrucciones de ejecución y arquitectura
```

---

## 2. Decisiones de Arquitectura y Diseño

- **Origen Principal:** Apache Kafka (`pedidos-ventas` en `127.0.0.1:9092`).
- **Origen Opcional:** Amazon Kinesis Data Streams (`pedidos-ventas-kinesis`, 2 shards en `us-east-1`).
- **Ventana Analítica:** Ventana *tumbling* de **5 minutos** agrupada por `region`.
- **Watermark:** Umbral de tolerancia al retraso de **10 minutos** sobre la columna de evento `kafka_time`.
- **Destino Silver:** Delta Lake en `/tmp/lake/silver/ventas_streaming`.
- **Sink y Semántica de Entrega:** `foreachBatch` ejecutando `MERGE INTO` sobre la clave compuesta `(window_start, window_end, region)` con `outputMode("update")`. Garantiza idempotencia real ante reintentos y reprocesamiento de micro-batches.
- **Tolerancia a Fallos:** Checkpointing automático en `/tmp/lake/checkpoints/lab2b-kafka` mediante log transaccional de micro-batches y state store.

---

## 3. Instrucciones de Ejecución

### 3.1 Prerrequisito: Clúster Kafka (Lab 2a)
Asegurar que los contenedores de Kafka estén en ejecución:
```bash
cd ../../lab2a-kafka
docker-compose up -d
```

### 3.2 Inyectar Datos con el Productor Kafka
```bash
python scripts/productor_kafka.py
```

### 3.3 Ejecutar el Pipeline de Streaming con Spark
```bash
python scripts/streaming_pipeline.py
```

### 3.4 Verificar la Tabla Delta en Silver
En una consola interactiva de Python:
```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.read.format("delta").load("/tmp/lake/silver/ventas_streaming").show(truncate=False)
```
Al re-ejecutar el productor de Kafka, las columnas `ventas_totales` y `num_pedidos` se actualizan sin duplicar registros para las ventanas existentes.
