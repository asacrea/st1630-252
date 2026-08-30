# Diseño Kafka — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 30/08/2026
**Estudiante:** Sebastian Andres Medina Cabezas - samedinac@eafit.edu.co

## Documentación Parte 0 - Exploración

### 1
Comando:
`docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092`
Salida:
`Created topic pedidos-ventas.`

El factor de replicación define cuántas copias idénticas de cada partición de un topic se almacenan en diferentes brokers del clúster para garantizar tolerancia a fallos, como localmente solo se cuenta con 1 broker, si configuro un número mayor que 1, Kafka intentará replicar los datos en otros brokers que no existen, por lo que la creación del topic fallará.

### 2

Comando:
`docker exec st1630-lab2a-kafka kafka-topics --bootstrap-server localhost:9092 --list`
Salida:
`pedidos-ventas`

No aparecen mas topics q el q creé.

### 3

Veo que estan listadas 4 particiones, y el lider es: "1", el cual hace referencia a `KAFKA_NODE_ID: 1` (esta en el docker-compose.yml), ya que fue el unico broker que se creo en docker y que esta corriendo.

### 4

Hubieran podido aparecer en particiones diferentes si la key hubiera sido diferente (Medellín en vez de Bogotá).
Kafka decide a que partición enviar los mensajes dependiendo de la formula `hash(key) % número_de_particiones`, entonces la key es la que decide para donde va. 

### 5

Me aparecieron en las particiones 0, 3  y 2 (mande 34 mensajes), no es el mismo comportamiento que con key fija.

## Significado del lag

¿Qué significa Lag = 0?
Significa que el consumidor ya procesó y commiteó todos los mensajes que el productor ha enviado hasta ese instante. Basicamente que no hay cola de espera.

¿Qué le pasa al lag si detienes el consumidor a mitad de proceso?

El Lag se congelaria o aumentaria, como el consumidor se apaga, deja de enviar commits a Kafka. Si los productores siguen mandando eventos al topic mientras el script está apagado, esos mensajes nuevos se van acumulando y el indicador de lag empezará a subir por cada registro que ingrese.


## Pregunta 1 — Garantía elegida

Elegiste at-least-once para este lab. Justifica en términos del modelo
de commit de offset y de la idempotencia del MERGE Delta: ¿qué pasa
exactamente si el consumidor falla después del MERGE pero antes del
commit? ¿Cuántas veces procesará Kafka ese mensaje? ¿Por qué el
resultado en Bronze es el mismo?

> Pista: esto es literalmente lo que probaste en la Parte 2.4 del lab
> (prueba de idempotencia) — cita tus propios números.

→ Si el consumidor falla después del MERGE pero antes del commit, el registro queda insertado en la capa Bronze de Delta Lake, pero Kafka nunca se entera de que el mensaje fue procesado exitosamente. Como resultado, en el próximo arranque de la aplicación, Kafka reenviará ese mismo mensaje.

Kafka procesará el mensaje 2 veces (o tantas veces como falle antes del commit). Sin embargo, el resultado en Bronze se mantiene intacto y sin duplicados gracias a que la operación MERGE de Delta Lake es idempotente. Al hacer match por la llave primaria pedido_id, Spark identifica que el registro ya existe en la tabla y simplemente lo omite (o lo actualiza con los mismos datos), evitando la duplicación.

```
Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

[OK] offset=15 partition=0 pedido_id=901                                        
[OK] offset=16 partition=0 pedido_id=903                                        
[OK] offset=17 partition=0 pedido_id=904                                        

Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.

21

Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

[OK] offset=17 partition=0 pedido_id=904                                        
[OK] offset=18 partition=0 pedido_id=999                                        

Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.

22
```

Prueba de Idempotencia:
- Conteo antes del reinicio: N = 21
- El consumidor procesó el "offset=17 (pedido_id=904)", pero se detuvo con Ctrl+C antes del commit.
- Al reiniciar, Kafka reenvió el "offset=17 (pedido_id=904)" y se procesó el nuevo "offset=18 (pedido_id=999)".
- Conteo final: N' = 22. 
La diferencia es de exactamente 1 registro (el nuevo pedido_id=999), demostrando que el offset 17 no se duplicó en Bronze.

## Pregunta 2 — Decisión de key

Elegiste `key=region` como clave del productor. Responde:

(a) ¿Qué garantía de orden provee?
(b) ¿Qué problema de balanceo genera, dado que Bogotá tiene ~40% del
    tráfico y el topic tiene 4 particiones?
(c) ¿Qué clave alternativa usarías si el orden no importara pero el
    balanceo fuera crítico? Justifica.

> Pista: tu script imprime un resumen región → partición → mensajes al
> final — úsalo como evidencia para (b), no una cifra inventada.

→ (a) Garantiza orden a nivel de región. Todos los pedidos de una misma región se enrutan siempre a la misma partición, asegurando que se procesen exactamente en el orden cronológico en que fueron producidos.

(b) Genera un problema de hot partitions. Como se observa en la evidencia, la partición 0 (P0) está recibiendo todo el tráfico de Bogotá (40%) y de Cali, acumulando 544 mensajes, lo que representa más de la mitad del tráfico total del clúster. Mientras tanto, la partición 3 (P3) está subutilizada con apenas 97 mensajes, generando un cuello de botella en P0.

(c) Si el orden cronológico por región no importara, la clave alternativa que usaria sería el pedido_id. Así nunca se repetirian las keys y la función de hash distribuiría los mensajes de forma equitativa entre las 4 particiones, balanceando la carga perfectamente.

```

Publicando 1000 pedidos en 'pedidos-ventas' (bootstrap: localhost:9092)...
  [100/1000] región=Bogotá       partición=0 offset=70
  [200/1000] región=Bucaramanga  partición=1 offset=48
  [300/1000] región=Medellín     partición=1 offset=78
  [400/1000] región=Bucaramanga  partición=1 offset=104
  [500/1000] región=Cali         partición=0 offset=282
  [600/1000] región=Bogotá       partición=0 offset=348
  [700/1000] región=Bucaramanga  partición=1 offset=183
  [800/1000] región=Bogotá       partición=0 offset=456
  [900/1000] región=Bogotá       partición=0 offset=503
  [1000/1000] región=Medellín     partición=1 offset=281

=== Resumen: región -> partición -> mensajes ===
  Bogotá         P0=400
  Medellín       P1=209
  Cali           P0=144
  Barranquilla   P3=97
  Bucaramanga    P1=73
  Otro           P2=77
```

## Pregunta 3 — Número de particiones

El topic tiene 4 particiones y el consumer group tiene 1 consumidor.
¿Cuántas particiones lee ese consumidor? ¿Cuál es el máximo de
consumidores activos que puedes añadir sin que ninguno quede ocioso?
¿Qué pasaría si añadieras 6?

→ Un solo consumidor lee las 4 particiones simultáneamente. El máximo de consumidores activos que se pueden añadir sin que ninguno quede ocioso es 3 (relación 1:1 entre consumidor y partición). Si añadieras 6 consumidores al mismo grupo, 4 consumidores procesarían una partición cada uno, y los 2 consumidores restantes quedarían completamente ociosos (inactivos), ya que una partición no puede ser leída por más de un consumidor del mismo grupo al mismo tiempo.

## Pregunta 4 — KRaft

El `docker-compose.yml` de este lab usa KRaft. Responde:

(a) ¿Qué hace KRaft que antes hacía el modelo de coordinación externa
    (previo a Kafka 4.0)?
(b) ¿Qué crees que pasaría si intentaras agregar un servicio de
    coordinación externa adicional al `docker-compose.yml` existente?
(c) ¿En qué momento del lab viste evidencia de que KRaft estaba
    funcionando? (pista: cualquier comando que le pregunte algo al
    clúster sin que exista un segundo servicio de coordinación corriendo)

→ (a) KRaft asume el rol de gestión de metadatos, elección de líderes y coordinación del clúster de manera interna utilizando un topic de quórum, eliminando la necesidad de usar ZooKeeper como servicio externo.

(b) Si intentara agregar un servicio de coordinación externo al docker-compose.yml, el clúster actual lo ignoraría por completo. Al estar configurado el broker nativamente en modo KRaft, no está diseñado para comunicarse con un quórum externo y funcionaría de forma completamente autónoma.

(c) Me di cuenta de que KRaft estaba funcionando cuando consulte los metadatos directamente contra el broker del clúster sin usar nada externo:

docker exec st1630-lab2a-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic pedidos-ventas

Este comando retornó con los líderes, réplicas e ISR del topic. 

## Pregunta 5 — Escalabilidad

Si el volumen de pedidos creciera 100× (de 1.000 a 100.000 mensajes
por lote), ¿qué tres cambios harías en este lab? Justifica cada uno
citando conceptos de S6:

(a) Un cambio en el productor
(b) Un cambio en el topic (particiones)
(c) Un cambio en el consumer group

→ (a) En el productor: Implementaria Batching. Aumentar los parámetros batch.size y linger.ms. Esto agruparia múltiples mensajes en un solo paquete de red antes de enviarlos, reduciendo drásticamente la latencia por I/O y la saturación de red.
(b) En el topic: Aumentaria el número de particiones a 20 o más. Esto incrementaria el paralelismo lógico del topic, permitiendo que la carga se fragmente más para no saturar los hilos de lectura.
(c) En el consumer group: Escalaria horizontalmente. Desplegar múltiples instancias del consumidor (hasta igualar el nuevo número de particiones) para que consuman en paralelo, distribuyendo el procesamiento de los DataFrames y los MERGE de Spark entre varias máquinas.
