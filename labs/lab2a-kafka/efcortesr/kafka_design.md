# Diseno Kafka - Lab 2a

**Curso:** ST1630-2026-2 - **Semana:** S6-S7 - **Fecha:** 2026-08-26  
**Estudiante:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio, Mariana Sanchez

## Evidencia de ejecucion

Infraestructura local levantada con `docker compose up -d`.

```text
st1630-lab2a-kafka     Up (healthy)   0.0.0.0:9092->9092/tcp
st1630-lab2a-kafka-ui  Up             0.0.0.0:8080->8080/tcp
```

Topic creado:

```powershell
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092
```

Descripcion del topic:

```text
Topic: pedidos-ventas  PartitionCount: 4  ReplicationFactor: 1
Partition: 0  Leader: 1  Replicas: 1  Isr: 1
Partition: 1  Leader: 1  Replicas: 1  Isr: 1
Partition: 2  Leader: 1  Replicas: 1  Isr: 1
Partition: 3  Leader: 1  Replicas: 1  Isr: 1
```

Resumen del productor:

```text
Bogota         P0=382
Medellin       P1=220
Cali           P0=149
Barranquilla   P3=88
Bucaramanga    P1=95
Otro           P2=66
```

Offsets finales del topic y lag del grupo `analytics-group`:

```text
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
0          531             531             0
1          315             315             0
2          66              66              0
3          88              88              0
```

Conteo final en Bronze Delta:

```text
COUNT=1000
_kafka_partition=0 -> 531
_kafka_partition=1 -> 315
_kafka_partition=2 -> 66
_kafka_partition=3 -> 88
```

## Parte 0 - Exploracion

1. El topic se creo con 4 particiones y factor de replicacion 1 usando el comando documentado arriba. En local el factor es 1 porque el `docker-compose.yml` solo levanta un broker; pedir factor 2 o 3 no tendria donde replicar. En produccion usaria factor 3 como base para tolerar fallos de brokers y mantener replicas ISR.

2. Al listar topics despues de consumir aparecen:

```text
__consumer_offsets
pedidos-ventas
```

`__consumer_offsets` es el topic interno donde Kafka guarda los offsets commiteados por los consumer groups. En este lab almacena, por ejemplo, los offsets del grupo `analytics-group`.

3. En `pedidos-ventas` hay 4 particiones. Como el cluster local tiene un solo broker, el lider de P0, P1, P2 y P3 es siempre el broker `1`; las replicas y el ISR tambien son `1`.

4. La misma propiedad de particionado con key fija se verifico con los 1.000 pedidos reales: cada valor de `region` fue siempre a una unica particion. Ejemplo: `Bogota -> P0=382`, `Otro -> P2=66`, `Barranquilla -> P3=88`.

5. Si los mensajes van sin key, Kafka no puede garantizar orden por region porque no existe una clave estable para asignarlos siempre a la misma particion. Asumimos que se usa round-robin por batches, para tener un mejor balanceo.

## Pregunta 1 - Garantia elegida

La garantia implementada es at-least-once. El consumidor tiene `enable_auto_commit=False` y solo ejecuta `commit_mensajes(...)` despues de que `merge_a_bronze(...)` termina sin excepcion. Si el consumidor falla despues del MERGE pero antes del commit, Kafka no registra ese offset como procesado y lo vuelve a entregar en el siguiente arranque.

La seguridad de reprocesar viene del MERGE Delta:

```text
existente.pedido_id = nuevo.pedido_id
```

En la prueba controlada procese `pedidos-ventas`, particion `2`, offset `0`. Primero hice MERGE y force un fallo antes del commit:

```text
[ERROR] offset=0 partition=2 no se commiteo -- se reprocesara.
Causa: Fallo controlado despues del MERGE y antes del commit
```

El conteo antes de reiniciar fue `N=1`. Luego reprocesé exactamente el mismo `partition=2 offset=0` y el consumidor commiteo:

```text
[OK] offset=0 partition=2 pedido_id=93dc8117-d207-4884-9d47-f795555fdb91 commit=1
```

El conteo despues fue `N'=1`. Kafka entrego el mensaje de nuevo, pero Bronze no duplico la fila porque el MERGE actualizo por `pedido_id`.

## Pregunta 2 - Decision de key

Use `key=region`. Eso garantiza orden dentro de cada region porque todos los pedidos con la misma region caen en la misma particion y Kafka preserva orden por particion.

El costo es balanceo desigual. En la ejecucion, `Bogota` concentro 382 mensajes en P0 y `Cali` tambien cayo en P0 con 149 mensajes. Por eso P0 termino con 531 mensajes, mientras P2 solo tuvo 66:

```text
P0=531, P1=315, P2=66, P3=88
```

Si el orden por region no importara y el balanceo fuera critico, usaria `key=pedido_id`. Como cada `pedido_id` es UUID, la distribucion hash tenderia a ser mas uniforme entre particiones, aunque ya no habria orden por region.

## Pregunta 3 - Numero de particiones

El topic tiene 4 particiones y el grupo `analytics-group` corrio con 1 consumidor; por tanto ese consumidor leyo las 4 particiones. El maximo de consumidores activos sin ociosidad es 4, porque Kafka asigna cada particion a un solo consumidor dentro del mismo grupo.

Si agregara 6 consumidores al mismo grupo, solo 4 consumirian datos y 2 quedarian ociosos. Para que 6 consumidores trabajen activamente se necesitarian al menos 6 particiones.

## Pregunta 4 - KRaft

KRaft reemplaza el modelo anterior donde Kafka dependia de un servicio externo de coordinacion. En este compose el broker tambien actua como controller:

```yaml
KAFKA_PROCESS_ROLES: "broker,controller"
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"
```

Si agregara un coordinador externo al compose actual no aportaria nada a este cluster KRaft; seria una pieza separada que el broker no usa. Incluso podria confundir la configuracion si se mezclan variables del modo antiguo con el modo KRaft.

Vi evidencia de que KRaft funcionaba cuando el broker respondio comandos de metadata sin ningun servicio adicional:

```powershell
docker exec st1630-lab2a-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic pedidos-ventas
```

El comando mostro lideres, replicas e ISR del topic usando solo el broker/controller KRaft.

## Pregunta 5 - Escalabilidad

Si el volumen creciera 100x, haria estos cambios:

(a) Productor: pasaria de envio sincronico mensaje a mensaje a envio asincronico con callbacks, manteniendo `acks="all"` e incrementando `batch_size`/`linger_ms` segun latencia aceptable. Eso mejora throughput sin bajar la garantia de durabilidad.

(b) Topic: aumentaria particiones. Con 4 particiones, P0 ya quedo como hot partition con 531 de 1000 mensajes. Para 100.000 mensajes, aumentaria particiones y revisaria la key si `region` sigue generando concentracion.

(c) Consumer group: levantaria varios consumidores en el mismo `analytics-group`, maximo uno activo por particion. Tambien mantendria procesamiento por micro-batches: un MERGE por batch y commit de offsets solo despues del MERGE exitoso.
