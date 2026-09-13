# Lab 2a - Productor/consumidor Kafka

**Estudiante:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio, Mariana Sanchez 
**Fecha de ejecucion:** 2026-08-26  
**Cluster local:** Kafka KRaft de 1 broker via `../docker-compose.yml`

## Configuracion local

Desde la raiz del repositorio:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r labs\lab2a-kafka\requirements.txt
cd labs\lab2a-kafka
docker compose up -d
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
```

Kafka UI queda disponible en:

```text
http://localhost:8080
```

## Ejecucion

```powershell
cd labs\lab2a-kafka\efcortesr\scripts
python productor_kafka.py
$env:MAX_MESSAGES='1000'
$env:BATCH_SIZE='100'
python consumidor_kafka.py
```

## Resultado ejecutado

- Productor: 1.000 mensajes publicados en `pedidos-ventas`.
- Consumidor: 1.000 registros en Bronze Delta.
- Lag final de `analytics-group`: 0 en las 4 particiones.
- Evidencia: `kafka_design.md`, `datos/prueba_idempotencia.md` y `datos/kafka_ui_lag_cero.png`.

En Windows, el consumidor configura automaticamente `HADOOP_HOME` si existe `hadoop/bin/winutils.exe` en la raiz del repositorio.
