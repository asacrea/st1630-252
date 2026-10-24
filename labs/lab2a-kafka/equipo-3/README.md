# Lab 2a — Productor/consumidor Kafka — Entrega

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha de entrega:** 31 de agosto de 2026
**Estudiantes:** Hellen Yanes Doria, Sebastian Salazar Henao, Andres
Felipe Velez Alvarez, Samuel Samper Cardona —
Hyanesd@eafit.edu.co, Ssalazarh3@eafit.edu.co, Afveleza@eafit.edu.co,
Ssamperc@eafit.edu.co

## Configuración del clúster usado

- **Sistema operativo del entorno de ejecución:** WSL2 (Ubuntu 26.04)
  sobre Windows 11. Se migró desde Windows nativo por fallos
  recurrentes de PySpark en ese entorno (ver `primor.md` para el
  detalle completo del troubleshooting).
- **Kafka:** `confluentinc/cp-kafka:7.6.0`, modo KRaft, 1 broker
  (`st1630-lab2a-kafka`), `CLUSTER_ID` corregido respecto al valor
  original del `docker-compose.yml` (bug de compatibilidad con la
  versión de imagen, no una decisión de diseño).
- **Kafka UI:** `provectuslabs/kafka-ui:latest`, puerto 8080.
- **Topic:** `pedidos-ventas`, 4 particiones, factor de replicación 1.
- **Python:** 3.11.16, gestionado con conda (entorno `lab2a`) dentro
  de WSL2.
- **PySpark:** 3.5.1 · **Delta Lake:** 3.1.0 · **Java:** OpenJDK 17.
- **Bronze:** `/tmp/lake/bronze/pedidos` (ruta por defecto del script,
  dentro del filesystem de WSL2/Ubuntu).

## Resumen de resultados

- **Productor:** 1.000 pedidos enviados con `key=region`,
  `acks='all'`. Resumen región → partición (última corrida completa
  verificada con lag=0):
  ```
  Bogotá         P0=368
  Medellín       P1=209
  Cali           P0=161
  Barranquilla   P3=101
  Bucaramanga    P1=72
  Otro           P2=84
  ```
- **Consumidor:** `enable_auto_commit=False`, commit manual después
  del MERGE exitoso, columnas de trazabilidad `_kafka_offset`,
  `_kafka_partition`, `_kafka_topic`, `_ingested_at` agregadas a cada
  fila de Bronze. Verificado con `Total lag = 0` en Kafka UI →
  Consumer Groups → `analytics-group` (ver
  `datos/kafka_ui_lag_cero.png`).
- **Prueba de idempotencia:** ejecutada y documentada en
  `datos/prueba_idempotencia.md` — N=109 antes de un `kill -9` a mitad
  de proceso, N'=144 después de reiniciar y reprocesar mensajes ya
  vistos, y confirmación definitiva de cero duplicados (total de filas
  = `pedido_id` distintos = 144).
- **`kafka_design.md`:** las 5 preguntas + Parte 0 respondidas citando
  evidencia real del propio pipeline.

## Cómo reproducir

```bash
docker-compose up -d
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas \
  --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092

# entorno Python (dentro de WSL2/Ubuntu)
conda create -n lab2a python=3.11 -y
conda activate lab2a
pip install kafka-python pyspark==3.5.1 delta-spark==3.1.0

export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0 pyspark-shell"

python scripts/productor_kafka.py
python scripts/consumidor_kafka.py   # Ctrl+C para detener
```

## Notas de troubleshooting relevantes para quien reproduzca esto

PySpark enWindows nativo requiere `winutils.exe`/`HADOOP_HOME`, tuvo 
problemas de "Python worker failed to connect" por el alias de Microsoft 
Store, y finalmente la JVM de Spark se cayó de forma irrecuperable durante
un MERGE largo. Migrar a WSL2 resolvió todos estos problemas de una vez.
