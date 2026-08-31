# Diseño Kafka — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 31 de agosto de 2026
**Estudiantes:** Hellen Yanes Doria, Sebastian Salazar Henao, Andres Felipe Velez Alvarez, Samuel Samper Cardona - Hyanesd@eafit.edu.co, Ssalazarh3@eafit.edu.co, Afveleza@eafit.edu.co, Ssamperc@eafit.edu.co

## Parte 0 — Exploración

1. **Comando exacto usado para crear el topic:**
   ```
   docker exec st1630-lab2a-kafka kafka-topics --create \
     --topic pedidos-ventas --partitions 4 --replication-factor 1 \
     --bootstrap-server localhost:9092
   ```
   El factor de replicación es 1 porque el clúster de este lab corre
   con un solo broker (`KAFKA_NODE_ID: 1`, único nodo en
   `docker-compose.yml`) — pedir un factor mayor sería imposible de
   cumplir, ya que no hay un segundo broker donde poner la copia. En
   producción, siguiendo el estándar visto en clase, el factor sería
   típicamente **3**, para poder tolerar la caída de hasta 2 brokers
   sin perder datos ni disponibilidad de escritura (siempre que el
   ISR mínimo se mantenga).

2. **Topic adicional no creado por mí:** `__consumer_offsets`. No
   apareció hasta que corrí `consumidor_kafka.py` por primera vez con
   `group_id="analytics-group"` — Kafka lo crea de forma perezosa la
   primera vez que un consumer group necesita persistir sus offsets
   commiteados. Reemplaza lo que en modelos más antiguos se guardaba
   en Zookeeper.

3. **Kafka UI → pedidos-ventas → Partitions:** 4 particiones (IDs 0-3),
   cada una con `Replicas=1` y el broker `1` como único líder de las 4
   (es el único broker del clúster, así que no hay elección real de
   liderazgo posible).

4. **3 mensajes con la misma key (`Bogota`):** los tres cayeron en la
   **misma partición (3)**, con offsets consecutivos 0, 1, 2
   (verificado en Kafka UI → Messages). No podrían haber caído en
   particiones distintas mientras la key sea idéntica: Kafka calcula
   la partición como `hash(key) % número_de_particiones`, una función
   determinística — misma key, mismo resultado, siempre.

5. **2 mensajes sin key, enviados en 3 tandas distintas:** cada tanda
   cayó en una partición diferente (2, luego 1, luego 3), pero los 2
   mensajes de una misma tanda (mismo proceso del producer) siempre
   cayeron juntos. Esto es el comportamiento del **sticky partitioner**
   de Kafka: sin key, no reparte mensaje por mensaje al azar, sino que
   agrupa varios mensajes en la misma partición por lote (mejor
   throughput de batching) y rota de partición entre lotes/procesos
   distintos — no es aleatorio puro, pero tampoco fijo como con key.

## Pregunta 1 — Garantía elegida

Elegí **at-least-once**, implementado con `enable_auto_commit=False` y
`consumer.commit()` llamado únicamente **después** de que
`merge_a_bronze()` termina sin excepción.

Si el consumidor falla **después** del MERGE pero **antes** del
commit, Kafka nunca se entera de que ese mensaje fue procesado (el
offset commiteado sigue apuntando al mensaje anterior). Al reiniciar
el consumidor, Kafka le vuelve a entregar ese mismo mensaje —
potencialmente más de una vez si el fallo se repite. El resultado en
Bronze es el mismo sin importar cuántas veces se reprocese, porque el
`MERGE ... ON pedido_id` es idempotente: la segunda ejecución hace
`whenMatchedUpdateAll()` en vez de insertar una fila nueva.

Lo comprobé en la Parte 2.4: maté el consumidor a la fuerza
(`kill -9`) en medio del procesamiento, con Bronze en **N=109** filas.
Al reiniciar, Kafka reentregó los offsets 0 al ~30 de la partición 1
(que ya habían sido procesados antes de morir), y el consumidor
también avanzó con mensajes nuevos de otras particiones — Bronze
terminó en **N'=144**. Comparar N vs N' directamente no prueba nada
por sí solo (subió tanto por reproceso como por mensajes genuinamente
nuevos), así que corrí la prueba definitiva:

```
Total filas: 144
Pedido_id distintos: 144
¿Hay duplicados? NO
```

Con el total de filas exactamente igual al total de `pedido_id`
distintos, queda demostrado que ningún mensaje —ni siquiera los que
Kafka reentregó por el `kill -9`— generó una fila duplicada en Bronze.

## Pregunta 2 — Decisión de key

Usé `key=region` en `producer.send(TOPIC, key=pedido["region"], ...)`.

**(a) Garantía de orden:** todos los pedidos de una misma región
siempre van a la misma partición (mismo `hash(region) % 4`), así que
Kafka garantiza que se lean en el mismo orden en que se escribieron
*dentro de esa región*. No hay garantía de orden global entre
regiones distintas.

**(b) Problema de balanceo:** con mi resumen real del productor
(1.000 pedidos):
```
Bogotá         P0=368
Medellín       P1=209
Cali           P0=161
Barranquilla   P3=101
Bucaramanga    P1=72
Otro           P2=84
```
Bogotá por sí sola representa 368/1000 (36.8%, consistente con el
peso ~40% que le di en el generador) y cae **toda** en la partición
0 — es una hot partition real. Además, como tengo 6 regiones mapeadas
a solo 4 particiones, `hash() % 4` produce colisiones inevitables:
Cali comparte la partición 0 con Bogotá (0+161=529 mensajes en esa
sola partición, más de la mitad del tráfico total), y Medellín
comparte la partición 1 con Bucaramanga. Esto agrava el desbalance
más allá de lo que "Bogotá = 40%" sugeriría por sí solo.

**(c) Alternativa si el orden no importara:** usaría `key=pedido_id`.
Como cada pedido tiene un UUID único, el hash se distribuye
prácticamente uniforme entre las 4 particiones, eliminando la hot
partition — a costa de perder cualquier noción de orden por región.

## Pregunta 3 — Número de particiones

El topic tiene 4 particiones y mi consumer group (`analytics-group`)
corrió con **1 consumidor**, que se asignó las 4 particiones
completas (confirmado en Kafka UI → Consumers → analytics-group:
`Assigned Partitions: 4`).

El máximo de consumidores activos sin que ninguno quede ocioso es
**4** — uno por partición, ya que Kafka nunca asigna más de un
consumidor por partición dentro del mismo consumer group. Si añadiera
un 5º consumidor al mismo grupo, ese consumidor quedaría sin ninguna
partición asignada — completamente ocioso, sin recibir mensajes,
hasta que otro consumidor del grupo caiga y libere una partición.

## Pregunta 4 — KRaft

**(a)** KRaft reemplaza el modelo de coordinación externa (Zookeeper,
previo a Kafka 4.0) integrando esa función directamente en el propio
broker vía un quorum Raft. En mi `docker-compose.yml`,
`KAFKA_PROCESS_ROLES: "broker,controller"` hace que el único broker
cumpla ambos roles a la vez.

**(b)** Si intentara agregar un segundo servicio de coordinación
externa (por ejemplo, un contenedor de Zookeeper) al
`docker-compose.yml` existente, entraría en conflicto directo con la
configuración KRaft ya activa — el clúster ya tiene su mecanismo de
coordinación resuelto internamente (`KAFKA_CONTROLLER_QUORUM_VOTERS`),
así que un Zookeeper adicional sería redundante y probablemente
causaría errores de arranque por gestionar metadata de clúster por
dos vías incompatibles a la vez.

**(c)** Vi evidencia de que KRaft funciona en **cualquier** comando
que le pregunté al clúster, sin que exista ningún segundo contenedor
de coordinación corriendo: `docker ps` muestra únicamente
`st1630-lab2a-kafka` y `st1630-lab2a-kafka-ui`, y aun así pude crear
el topic, listar particiones, ver el líder de cada una en Kafka UI, y
crear/borrar el topic varias veces durante mis pruebas — toda esa
metadata (líder=broker 1 en las 4 particiones) se resolvió
internamente por el propio broker actuando como controller.

## Pregunta 5 — Escalabilidad 100×

Si el volumen pasara de 1.000 a 100.000 mensajes por lote:

**(a) Productor:** cambiaría a envío **asíncrono**
(`future.add_callback()` en vez de `future.get()` bloqueante en cada
mensaje) — con 100.000 mensajes, esperar la confirmación uno por uno
de forma síncrona sería un cuello de botella severo de latencia
acumulada; el modo asíncrono permite que Kafka confirme en paralelo
mientras el productor sigue enviando.

**(b) Topic:** subiría el número de particiones — por ejemplo a 12 (o
un múltiplo de mis 6 regiones, para reducir las colisiones de key que
ya identifiqué en la Pregunta 2), y en un clúster real de producción
con varios brokers, subiría el factor de replicación a 3 para
tolerancia a fallos real, ya que con más volumen la pérdida de datos
sería más costosa.

**(c) Consumer group:** añadiría más instancias del consumidor,
escalando hasta igualar el número de particiones (siguiendo la regla
de la Pregunta 3: N particiones = máximo N consumidores activos en
paralelo sin ociosidad). Con 12 particiones, podría llegar hasta 12
consumidores procesando en paralelo, multiplicando el throughput de
ingesta a Bronze.