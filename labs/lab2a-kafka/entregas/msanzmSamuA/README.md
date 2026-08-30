# Entregable Lab 2a — Productor / Consumidor Kafka con KRaft & Delta Lake

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 28/08/2026  
**Equipo:** Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango (`sarangoe3@eafit.edu.co`), Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

---

## Estructura de la Entrega

```
labs/lab2a-kafka/entregas/msanzmSamuA/
├── scripts/
│   ├── productor_kafka.py      # Productor con key=region, acks='all' y resumen de particiones
│   └── consumidor_kafka.py     # Consumidor at-least-once con enable_auto_commit=False y MERGE Delta
├── datos/
│   ├── prueba_idempotencia.md  # Evidencia empírica N = N' con logs y conteos
│   └── kafka_ui_lag_cero.png   # Evidencia de consumo completo (Lag = 0) en Kafka UI
├── kafka_design.md             # Parte 0 (Exploración) + 5 preguntas de diseño técnico
├── bitacora_delegacion.md      # Registro de delegación e IA (política del curso)
└── README.md                   # Resumen de la entrega y configuración
```

---

## Configuración del Clúster y Ejecución

- **Modo de Coordinación**: Apache Kafka 7.6.0 operando en modo **KRaft** (sin Zookeeper).
- **Topic**: `pedidos-ventas` (4 particiones, `replication-factor 1`).
- **Consumer Group**: `analytics-group`.
- **Ruta Bronze Delta Lake**: `/tmp/lake/bronze/pedidos`.

### Comandos de Ejecución

1. **Levantar Infraestructura Local**:
   ```bash
   docker-compose up -d
   ```
2. **Crear Topic (Parte 0)**:
   ```bash
   docker exec st1630-lab2a-kafka kafka-topics --create \
     --topic pedidos-ventas --partitions 4 --replication-factor 1 \
     --bootstrap-server localhost:9092
   ```
3. **Ejecutar Productor**:
   ```bash
   python3 scripts/productor_kafka.py
   ```
4. **Ejecutar Consumidor**:
   ```bash
   python3 scripts/consumidor_kafka.py
   ```
