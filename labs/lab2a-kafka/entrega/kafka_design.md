# Diseño Kafka — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 31/08/2026
**Estudiantes:**
 _Mateo García Carreño / mgarciac10@eafit.edu.co_  
 _Juan José Gomez / jjgomezv2@eafit.edu.co_  
 _Juan José Vargas / jjvargasl@eafit.edu.co_  
 _Luis Moreno Gutierrez_

> Cada pregunta trae abajo un bloque **“Evidencia de nuestra ejecución”**

---

## Anexo — Parte 0: exploración antes de escribir código

### 0.1 Creación del topic

Comando exacto que usamos:

```bash
docker exec st1630-lab2a-kafka kafka-topics --create \
  --topic pedidos-ventas --partitions 4 --replication-factor 1 \
  --bootstrap-server localhost:9092
# Created topic pedidos-ventas.
```

Verificación:

```
Topic: pedidos-ventas  TopicId: 5FC6Hc6RQP-DLRv_Ed8pxw  PartitionCount: 4  ReplicationFactor: 1
    Topic: pedidos-ventas  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
    Topic: pedidos-ventas  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
    Topic: pedidos-ventas  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
    Topic: pedidos-ventas  Partition: 3  Leader: 1  Replicas: 1  Isr: 1
```

**¿Por qué el factor de replicación es 1 en local y qué sería en producción?**

→ Porque el clúster local solo tiene 1 broker, entonces no hay dónde más replicar. En producción se usa mínimo RF=3, con al menos 3 brokers, para poder perder un broker sin perder datos ni tumbar la disponibilidad del topic.

### 0.2 Los topics que aparecen en la lista

Antes de crear nada, `--list` devolvía vacío. Después de crear el topic
devolvía solo `pedidos-ventas`. Después de correr el consumidor por
primera vez, aparece un topic más que nosotros no creamos:

```
__consumer_offsets
pedidos-ventas
```

```
Topic: __consumer_offsets  PartitionCount: 50  ReplicationFactor: 1
Configs: compression.type=producer, cleanup.policy=compact, segment.bytes=104857600
```

**¿Para qué sirve `__consumer_offsets`?** (nótese que apareció solo
después de que `analytics-group` hizo su primer commit, y que su
`cleanup.policy` es `compact`)

→ Es un topic interno donde Kafka guarda los offsets que cada consumer group va confirmando por partición. Por eso aparece solo hasta que analytics-group hace su primer commit, antes no había nada que persistir.Y su cleanup.policy es compact porque a Kafka no le importa el historial de commits, solo el último offset, entonces compaction va botando los valores viejos y deja solo el más reciente por key.

### 0.3 Kafka UI — Topics → pedidos-ventas → Partitions

Lo que se ve en [http://localhost:8080](http://localhost:8080), y que
coincide con el `--describe` de arriba: 4 particiones (0, 1, 2, 3), y el
**broker 1 es líder de las 4** (es el único broker del clúster), con
`Replicas: 1` e `ISR: 1` en todas.

→ La pestaña Partitions muestra las 4 particiones (0, 1, 2 y 3) y en todas el líder es el broker 1, que es el único que existe en el clúster. Cada una aparece con una sola réplica ([1]) y esa misma réplica en ISR, o sea 4/4 in-sync y 0 under-replicated partitions, que es lo esperado con replication factor 1. Lo que sí cambia entre particiones es el volumen: la P0 llega hasta el offset 543 mientras que la P2 apenas tiene 58, que es el desbalance que produce key=region

### 0.4 Tres mensajes con la misma key

```bash
printf 'Bogotá:{"nota":"prueba 1"}\nBogotá:{"nota":"prueba 2"}\nBogotá:{"nota":"prueba 3"}\n' \
| docker exec -i st1630-lab2a-kafka kafka-console-producer \
    --topic pedidos-ventas --bootstrap-server localhost:9092 \
    --property "parse.key=true" --property "key.separator=:"
```

Offsets finales por partición, antes y después del envío:

```
antes:    P0=0  P1=0  P2=0  P3=0
después:  P0=3  P1=0  P2=0  P3=0     <- los 3 cayeron en la partición 0
```

**¿Podrían haber caído en particiones distintas? ¿Por qué sí o por qué no?**

→ No podían caer en particiones distintas, porque al tener key Kafka aplica un hash sobre esa key para escoger partición, y mismo key siempre da el mismo resultado (mientras el topic no cambie su número de particiones). Por eso los 3 mensajes con key "Bogotá" fueron todos a la partición 0.

### 0.5 Mensajes sin key

Enviamos 2 mensajes sin key, tres veces seguidas (cada ronda es una
invocación nueva de `kafka-console-producer`):

```
tras ronda 1 (2 msjs sin key): P0=3  P1=0  P2=0  P3=2
tras ronda 2 (2 msjs sin key): P0=3  P1=2  P2=0  P3=2
tras ronda 3 (2 msjs sin key): P0=3  P1=4  P2=0  P3=2
```

Es decir: dentro de una misma ronda los 2 mensajes siempre cayeron
juntos en la misma partición, pero la partición elegida cambió entre
rondas (P3, luego P1, luego P1 otra vez) — nunca fue igual de estable
que con key fija.

**¿Es siempre el mismo comportamiento que con key fija? ¿Qué hace Kafka
cuando la key es `None`?**

→ No es el mismo comportamiento. Con key fija se hace un hash de la key que siempre va a la misma partición. Sin key, Kafka reparte entre las particiones disponibles, por eso los 2 mensajes de una misma ronda caen juntos, pero de una ronda a otra cambia la partición elegida, porque no hay key que fije nada.

> Nota: después de esta exploración borramos y volvimos a crear el topic,
> para que los 9 mensajes de prueba (que no tienen `pedido_id`) no
> entraran a Bronze cuando corriéramos el consumidor.

---

## Pregunta 1 — Garantía elegida

Elegiste at-least-once para este lab. Justifica en términos del modelo
de commit de offset y de la idempotencia del MERGE Delta: ¿qué pasa
exactamente si el consumidor falla después del MERGE pero antes del
commit? ¿Cuántas veces procesará Kafka ese mensaje? ¿Por qué el
resultado en Bronze es el mismo?

**Evidencia de nuestra ejecución** (detalle completo en
`datos/prueba_idempotencia.md`):

- El `kill -9` de la prueba cayó **exactamente** en la ventana entre el
  MERGE y el commit: el log de run1 tiene 16 líneas `[OK]` (offsets 0–15),
  el grupo quedó commiteado en `CURRENT-OFFSET = 16`, y Bronze tenía 17
  filas, con offsets 0 a **16**. El offset 16 estaba escrito en Bronze
  pero sin commitear.
- Al reiniciar, Kafka reentregó los offsets 11 a 16 (rebobinamos 5
  posiciones para ampliar la ventana). Los `pedido_id` del log de run2
  son idénticos a los de run1.
- Conteo de Bronze: **N = 17 antes**, **N' = 17 después**, 17 `pedido_id`
  distintos, 0 duplicados.
- `_ingested_at` de los offsets 11–16 saltó de `18:19–18:20` (run1) a
  `18:22` (run2): el MERGE entró por `whenMatchedUpdateAll()`.
- **A escala completa:** sumando las 6 corridas del consumidor se
  ejecutaron **1.512 MERGE** (1.005 con commit + 507 sin commit por
  `CommitFailedError`) para **1.000 mensajes únicos** — o sea 512
  mensajes procesados más de una vez. Bronze terminó con **1.000 filas
  y 1.000 `pedido_id` distintos**, con los offsets completos de las 4
  particiones (543 + 316 + 58 + 83).

→ Con at-least-once el commit ocurre después del merge, así que si el consumidor muere entre ambos pasos (como nos pasó exactamente, con offset 16 escrito en Bronze pero sin commit), Kafka no sabe que ese mensaje ya se procesó y lo reentrega desde el último offset confirmado; no hay tope fijo de reintentos, cada reinicio sin commit exitoso vuelve a reentregar la ventana. El resultado en Bronze es el mismo porque el MERGE es idempotente sobre pedido_id: reprocesar no inserta fila nueva, solo sobreescribe la misma fila, por eso terminamos con N=N'=17 sin duplicados.

## Pregunta 2 — Decisión de key

Elegiste `key=region` como clave del productor. Responde:

(a) ¿Qué garantía de orden provee?
(b) ¿Qué problema de balanceo genera, dado que Bogotá tiene ~40% del
    tráfico y el topic tiene 4 particiones?
(c) ¿Qué clave alternativa usarías si el orden no importara pero el
    balanceo fuera crítico? Justifica.

**Evidencia de nuestra ejecución** — resumen región → partición de los
1.000 pedidos:

| Región | Partición | Mensajes |
|---|---|---:|
| Bogotá | **P0** | 389 |
| Cali | **P0** | 154 |
| Medellín | P1 | 227 |
| Bucaramanga | P1 | 89 |
| Barranquilla | P3 | 83 |
| Otro | P2 | 58 |

Offsets finales por partición:

```
pedidos-ventas:0:543     <- 54,3 % del tráfico
pedidos-ventas:1:316     <- 31,6 %
pedidos-ventas:2:58      <-  5,8 %
pedidos-ventas:3:83      <-  8,3 %
```

Dato clave: cada región cayó **siempre** en una sola partición (ninguna
región aparece en dos), y **Bogotá y Cali colisionaron en P0** — la hot
partition terminó peor que el 40 % que uno esperaría solo por Bogotá.

→ Con key=region, Kafka garantiza orden solo dentro de cada región: como el hash es determinístico, todos los pedidos de una misma región van siempre a la misma partición, así que se pueden consumir en el orden en que se produjeron. El problema de balanceo es que 6 regiones sobre 4 particiones fuerza colisiones, y en nuestro caso Bogotá y Cali cayeron juntas en P0, dejando esa partición con 543 mensajes (54,3% del tráfico) frente a apenas 58 en P2. Si el orden no importara pero el balanceo fuera crítico, usaríamos key=pedido_id, porque al ser 1.000 keys únicas el hash reparte casi uniforme entre las 4 particiones (~250 c/u), en vez de agrupar por una dimensión tan desbalanceada como la región.

## Pregunta 3 — Número de particiones

El topic tiene 4 particiones y el consumer group tiene 1 consumidor.
¿Cuántas particiones lee ese consumidor? ¿Cuál es el máximo de
consumidores activos que puedes añadir sin que ninguno quede ocioso?
¿Qué pasaría si añadieras 6?

**Evidencia de nuestra ejecución** — `kafka-consumer-groups --describe`
con el consumidor corriendo: un único `CONSUMER-ID` aparece asignado a
las 4 particiones a la vez:

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID
analytics-group pedidos-ventas  0          74              543             469   kafka-python-2.2.15-2ffd13d0...
analytics-group pedidos-ventas  1          0               316             316   kafka-python-2.2.15-2ffd13d0...
analytics-group pedidos-ventas  2          0               58              58    kafka-python-2.2.15-2ffd13d0...
analytics-group pedidos-ventas  3          0               83              83    kafka-python-2.2.15-2ffd13d0...
```

→ Con 1 solo consumidor, el --describe muestra el mismo consumer-id en las 4 filas, así que lee las 4 particiones a la vez. El máximo de consumidores activos sin que ninguno quede ocioso es 4, porque Kafka reparte por partición completa y nunca comparte una entre dos consumidores del mismo grupo. Si añadiéramos 6, solo 4 tomarían una partición cada uno y los otros 2 quedarían ociosos sin nada que consumir.

## Pregunta 4 — KRaft

El `docker-compose.yml` de este lab usa KRaft. Responde:

(a) ¿Qué hace KRaft que antes hacía el modelo de coordinación externa
    (previo a Kafka 4.0)?
(b) ¿Qué crees que pasaría si intentaras agregar un servicio de
    coordinación externa adicional al `docker-compose.yml` existente?
(c) ¿En qué momento del lab viste evidencia de que KRaft estaba
    funcionando?

**Evidencia de nuestra ejecución:**

- `docker ps` muestra exactamente **dos** contenedores —
  `st1630-lab2a-kafka` y `st1630-lab2a-kafka-ui` — y ningún servicio de
  coordinación aparte. El broker declara los dos roles:
  `KAFKA_PROCESS_ROLES: "broker,controller"` y
  `KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093"`.
- El quórum de metadata se puede consultar al propio broker:

  ```
  $ docker exec st1630-lab2a-kafka kafka-metadata-quorum \
      --bootstrap-server localhost:9092 describe --status
  ClusterId:              li8qjcjaTny7YQ_IRHUdmg
  LeaderId:               1
  LeaderEpoch:            1
  HighWatermark:          2508
  MaxFollowerLag:         0
  CurrentVoters:          [1]
  CurrentObservers:       []
  ```

- Evidencia “en negativo”, la más contundente: el broker **no arrancó**
  hasta que le dimos un `CLUSTER_ID` válido. Con el valor que traía el
  compose original abortaba en el preflight:

  ```
  ===> Running in KRaft mode, skipping Zookeeper health check...
  ===> Using provided cluster id st1630lab2aKRaftClusterID ...
  Cluster ID string st1630lab2aKRaftClusterID does not appear to be a valid UUID:
  Input string with prefix `st1630lab2aKRaftClusterI` is too long to be decoded as a base64 UUID
  ```

  (ver `../docker-compose.override.yml` y la sección de incidencias del
  `README.md` de esta entrega)

→ Básicamente KRaft hace que el mismo broker se encargue de guardarse su propia información de estado (como quién es el líder, qué particiones hay, etc.), en vez de depender de un servicio aparte para eso. Por eso al hacer docker ps solo salen 2 contenedores, el broker y la UI, no hay nada más corriendo. De hecho intentar meter otro servicio para eso no tendría mucho sentido, porque el broker ya no está armado para hablar con algo externo. Nos dimos cuenta de que esto sí estaba funcionando en dos momentos: primero cuando el broker literalmente no quiso prender hasta que le pusimos un CLUSTER_ID correcto, y en el log decía que estaba corriendo en modo KRaft; y después cuando corrimos el comando de metadata-quorum y nos devolvió quién era el líder y quién estaba en el quórum, o sea que esa parte la estaba manejando el broker solo.

## Pregunta 5 — Escalabilidad

Si el volumen de pedidos creciera 100× (de 1.000 a 100.000 mensajes
por lote), ¿qué tres cambios harías en este lab? Justifica cada uno
citando conceptos de S6:

(a) Un cambio en el productor
(b) Un cambio en el topic (particiones)
(c) Un cambio en el consumer group

**Evidencia de nuestra ejecución** (medidas reales, para dimensionar el
100×):

- **Productor, modo síncrono** (`future.get()` por mensaje):
  1.000 mensajes en **20,4 s** ≈ 49 msg/s. A 100.000 mensajes serían
  ~34 min solo de publicación.
- **Consumidor, un MERGE Delta por mensaje**: ~2 s por mensaje con la
  tabla compactada, ~6 s tras unos 90 mensajes y **varios minutos** con
  más de 1.000 archivos Parquet acumulados en Bronze — cada MERGE tiene
  que listar y escanear todo lo que dejaron los MERGE anteriores.
  Drenar los 1.000 mensajes nos tomó 6 corridas y dos rondas de
  `OPTIMIZE`/`VACUUM`.
- **La lentitud rompió el consumer group:** con `max_poll_records=500`
  (el default), procesar un lote de a un MERGE por mensaje supera
  `max_poll_interval_ms` (5 min), el broker expulsa al consumidor y
  **todos** los `commit()` posteriores fallan con `CommitFailedError`
  (507 de ellos en total). Lo arreglamos con `max_poll_records=10`.
- El desbalance de P0 (54,3 % del tráfico) se multiplica igual por 100:
  la hot partition es el cuello de botella que primero satura.

→ Lo primero que cambiaría es el productor: pasar de síncrono a asíncrono, porque con 100.000 mensajes esperar la respuesta uno por uno (como nos tomó los 20 s para 1.000) se volvería un cuello de botella enorme. En el topic, aumentar particiones no sirve de mucho si seguimos usando key=region, porque el problema no es cuántas particiones haya sino que solo existen 6 regiones posibles, entonces P0 seguiría acaparando el tráfico igual; tocaría cambiar la key a algo más repartido. Y en el consumer group el cambio más urgente es dejar de hacer un merge por cada mensaje individual, porque eso fue justo lo que nos rompió el consumidor en la corrida real (se demoraba tanto que Kafka lo expulsaba del grupo y los commits empezaban a fallar); en vez de eso habría que procesar por lotes y de paso meter más consumidores, hasta el límite de 4 que da el número de particiones.

---

## Evidencia adicional (Parte 2.5) — lag del consumer group

Captura de Kafka UI con `lag = 0`: `datos/kafka_ui_lag_cero.png`.
El mismo estado por CLI:

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
analytics-group pedidos-ventas  0          543             543             0    kafka-python-2.2.15-3900d335...
analytics-group pedidos-ventas  1          316             316             0    kafka-python-2.2.15-3900d335...
analytics-group pedidos-ventas  2          58              58              0    kafka-python-2.2.15-3900d335...
analytics-group pedidos-ventas  3          83              83              0    kafka-python-2.2.15-3900d335...
```

**Qué significa `lag = 0` y qué le pasa al lag si detienes el consumidor
a mitad de proceso:**

→ lag=0 significa que el consumidor ya leyó todo lo que hay en el topic, es decir CURRENT-OFFSET quedó igual al LOG-END-OFFSET en las 4 particiones, no le queda nada pendiente por consumir. Si detuviéramos el consumidor a mitad de proceso, el lag empezaría a subir apenas lleguen mensajes nuevos, porque el LOG-END-OFFSET seguiría creciendo con lo que se va publicando mientras el CURRENT-OFFSET se queda quieto en el último commit que alcanzó a hacer; ese lag es justo la forma de saber cuánto se está atrasando o si el consumidor se cayó.
