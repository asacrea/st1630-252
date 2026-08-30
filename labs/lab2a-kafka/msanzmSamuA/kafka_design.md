# Diseño Kafka — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 28/08/2026  
**Equipo:** Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango (`sarangoe3@eafit.edu.co`), Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

---

## Parte 0 — Anexo de Exploración

### 1. Creación del topic `pedidos-ventas`
**Comando ejecutado:**
```bash
docker exec st1630-lab2a-kafka kafka-topics --create \
  --topic pedidos-ventas \
  --partitions 4 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

- **¿Por qué el factor es 1 en local?**  
  Porque el entorno local de Docker cuenta con un único broker Kafka (`st1630-lab2a-kafka`). Dado que un factor de replicación exige asignar réplicas en distintos brokers miembros del ISR, un factor mayor a 1 fallaría inmediatamente con `InvalidReplicationFactorException`.
- **¿Por qué sería distinto en producción?**  
  En un entorno de producción se utiliza un factor de replicación de **3** con `min.insync.replicas=2` sobre un clúster distribuido multi-broker. Esto garantiza la alta disponibilidad del quórum y la tolerancia a fallos ante la caída de hasta un broker sin pérdida de datos ni interrupción de escrituras.

### 2. Listado de topics y topic del sistema
**Comando ejecutado:**
```bash
docker exec st1630-lab2a-kafka kafka-topics --bootstrap-server localhost:9092 --list
```
**Output obtenido:**
- `pedidos-ventas`
- `__consumer_offsets`

- **¿Para qué sirve `__consumer_offsets`?**  
  Es un topic interno del sistema de Kafka utilizado para almacenar de forma duradera y distribuida los commits de offsets de cada *Consumer Group* (como `analytics-group`) por cada partición. Reemplaza el antiguo almacenamiento de offsets en Zookeeper.

### 3. Inspección en Kafka UI
- **Vista en Kafka UI (`http://localhost:8080`)**:  
  Navegando a **Topics → pedidos-ventas → Partitions**, se observan **4 particiones** (Indexadas `0`, `1`, `2`, `3`). En el modelo KRaft monopunto local, el **Broker 1** figura como el líder (*Leader*) e ISR único de las 4 particiones.

### 4. Prueba con claves fijas (`key="Bogotá"`)
**Mensajes enviados:**
```
Bogotá:{"nota":"prueba 1"}
Bogotá:{"nota":"prueba 2"}
Bogotá:{"nota":"prueba 3"}
```
- **Resultado observado:** Los 3 mensajes aparecieron exactamente en la misma partición (ej. Partición 2).
- **¿Podrían haber aparecido en particiones distintas?**  
  No. Kafka aplica la función de hashing `murmur2(key) % num_particiones`. Al ser la clave `"Bogotá"` idéntica en los 3 mensajes, el resultado del hash es determinista y asigna siempre la misma partición.

### 5. Prueba sin clave (`key=None`)
- **Resultado observado:** Los mensajes se distribuyeron de forma alternada entre distintas particiones (0, 1, 3).
- **Comportamiento:** Al no especificar una clave, Kafka utiliza el *Sticky Partitioner* (o *round-robin* por lotes), maximizando el balanceo de carga entre particiones.

---

## Pregunta 1 — Garantía elegida

En este laboratorio implementamos la garantía **at-least-once** mediante la coreografía de `enable_auto_commit=False` y la ejecución del commit manual (`consumer.commit()`) **únicamente después** de que `merge_a_bronze()` finaliza sin excepciones.

- **¿Qué pasa si el consumidor falla después del MERGE pero antes del commit?**  
  El offset del mensaje procesado no se habrá registrado en `__consumer_offsets`. Al reiniciar el consumidor, Kafka reenviará el mensaje desde el último offset commiteado.
- **¿Cuántas veces procesará Kafka ese mensaje?**  
  El mensaje será entregado y procesado por el consumidor **al menos dos veces**.
- **¿Por qué el resultado en Bronze es el mismo?**  
  Porque la función `merge_a_bronze()` realiza una operación `MERGE INTO` en Delta Lake evaluando la condición `existente.pedido_id = nuevo.pedido_id`. Si el mensaje ya fue insertado previamente en la tabla Delta, la condición se cumple y ejecuta `whenMatchedUpdateAll()` en lugar de duplicar la fila. Esta **idempotencia en el receptor (Delta Lake)** neutraliza los duplicados introducidos por el reenvío de Kafka.

---

## Pregunta 2 — Decisión de key

Se eligió `key=region` como clave de particionamiento del productor.

**(a) Garantía de orden:**  
Garantiza **orden secuencial estricto por región**. Todos los eventos pertenecientes a la misma región (ej. `"Bogotá"`) se procesan y leen exactamente en el mismo orden temporal en el que fueron publicados por el productor.

**(b) Problema de balanceo (*Hot Partition*):**  
Dado que Bogotá concentra ~40% del tráfico total (`PESOS_REGION = [0.40, ...]`), la partición a la que es mapeada `"Bogotá"` recibe 400 de los 1.000 mensajes enviados, mientras que otras particiones reciben una carga sensiblemente menor.  

**Evidencia de ejecución real (1,000 pedidos publicados):**
```text
=== Resumen: región -> partición -> mensajes ===
  Bogotá         P0=410
  Medellín       P1=191
  Cali           P0=156
  Barranquilla   P3=106
  Bucaramanga    P1=73
  Otro           P2=64
```
*La partición P0 concentra el 56.6% del volumen total del topic (Bogotá 410 + Cali 156), creando una hot partition por desbalanceo de claves.*

**(c) Clave alternativa si el orden no importara:**  
Se utilizaría `key=pedido_id` (UUID único por pedido) o `key=None`. Al asignar una clave única o nula por evento, `murmur2(pedido_id)` distribuye los 1.000 mensajes de forma perfectamente homogénea (250 mensajes por partición), eliminando el cuello de botella de la *hot partition*.

---

## Pregunta 3 — Número de particiones

En la configuración inicial, el topic `pedidos-ventas` tiene 4 particiones y el *consumer group* `analytics-group` cuenta con 1 consumidor.

- **¿Cuántas particiones lee ese consumidor?**  
  El único consumidor lee las **4 particiones** simultáneamente.
- **¿Cuál es el máximo de consumidores activos sin ociosidad?**  
  El máximo es **4 consumidores activos** (asignación 1:1 de 1 partición por consumidor).
- **¿Qué pasaría si se añaden 6 consumidores al mismo grupo?**  
  Kafka asignará 1 partición a cada uno de los primeros 4 consumidores, y los **2 consumidores restantes quedarán ociosos (*idle*)**, sin recibir ningún mensaje hasta que alguno de los consumidores activos caiga o se realice un rebalanceo.

---

## Pregunta 4 — KRaft

**(a) ¿Qué hace KRaft que antes hacía la coordinación externa (Zookeeper)?**  
KRaft (*Kafka Raft Metadata Mode*) gestiona el quórum de metadatos del clúster, la elección del nodo Controller, el estado de las particiones y los temas directamente dentro del motor de Kafka usando un log interno de eventos Raft, eliminando la latencia y complejidad de mantener un clúster Zookeeper externo.

**(b) ¿Qué pasaría si se agrega un servicio de coordinación externa adicional?**  
Al estar configurado el broker con `KAFKA_PROCESS_ROLES: "broker,controller"`, el broker operará en modo KRaft nativo e ignorará el servicio externo o fallará por incompatibilidad de puertos y roles.

**(c) Evidencia de funcionamiento de KRaft:**  
Se evidenció al ejecutar la creación de temas (`kafka-topics --create`) apuntando directamente al puerto del broker `localhost:9092`, sin necesidad de pasar la bandera `--zookeeper localhost:2181` que requerían las versiones anteriores a Kafka 3.x/4.0.

---

## Pregunta 5 — Escalabilidad 100×

Para escalar el pipeline de 1.000 a 100.000 mensajes por lote:

**(a) Cambio en el Productor:**  
Implementar envío asíncrono con callbacks (`future.add_callback()`), incrementar `batch.size` (ej. a 128 KB) y `linger.ms` (ej. 50 ms), y activar compresión de mensajes (`compression.type='lz4'` o `'zstd'`) para maximizar el *throughput* de red.

**(b) Cambio en el Topic:**  
Aumentar el número de particiones del topic de 4 a **16 o 32 particiones**, permitiendo que la carga de ingestión masiva se paralelice a través de múltiples discos y brokers.

**(c) Cambio en el Consumer Group:**  
Escalar horizontalmente la aplicación consumidora añadiendo instancias al `analytics-group` (hasta igualar las 16 o 32 particiones del topic) y migrar el procesamiento del consumidor a **Spark Structured Streaming** ejecutado en un clúster distribuido EMR.
