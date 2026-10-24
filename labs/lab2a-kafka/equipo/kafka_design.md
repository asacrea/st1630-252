# Diseño Kafka — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 2026-08-31
**Estudiantes:**
Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co · Juan Simón Ospina Martínez — jsospinam@eafit.edu.co
Sebastián Durán Fernández — sduranf@eafit.edu.co · Daniel Arcila Salazar — darcilas1@eafit.edu.co

> Todas las cifras salen de nuestra propia ejecución. Evidencia cruda en `datos/`: `parte0_exploracion.txt`,
> `parte0_consumer_offsets.txt`, `productor_output.txt`, `consumidor_output.txt`, `prueba_idempotencia.md`,
> `consumidor_expulsion_evidencia.txt`, `benchmark_sync_vs_async.txt`, `verificacion_final_bronze.txt`.

## Pregunta 1 — Garantía elegida

**at-least-once**, y no por preferencia: es la consecuencia de dos decisiones que se sostienen mutuamente.
`enable_auto_commit=False` nos da el control de *cuándo* Kafka considera leído un mensaje, y el `MERGE ... ON
pedido_id` hace que no importe si un mensaje llega dos veces.

### Qué pasa si el consumidor falla entre el MERGE y el commit

No lo razonamos en abstracto: **nos pasó**. Usamos `SIGKILL` y no `Ctrl+C` a propósito, porque `SIGINT` lo
atrapa el `except KeyboardInterrupt` y el script sale ordenadamente; `SIGKILL` no se puede atrapar y mata el
proceso en el instante exacto, que es el escenario real de "el consumidor se cayó" (`exitcode=137` = 128 + 9).
El log terminaba en 15 líneas `[OK]` (offsets 0–14 de P1), pero Bronze tenía **16 filas**:

```
=== CONTEO BRONZE [ANTES de reiniciar] ===
  filas_totales = 16 · pedido_id_distintos = 16 · duplicados = 0 · offset_max = 15 (partición 1)

Consumer group 'analytics-group' has no active members.
GROUP           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
analytics-group 1          15              263             248
```

Esa diferencia de uno es toda la respuesta: el `offset=15` **completó su MERGE** —por eso Bronze lo tiene—
pero el proceso murió **antes** del `consumer.commit()` y antes de imprimir su `[OK]`; murió dentro de la
ventana crítica sin que tuviéramos que forzarlo. Y del otro lado, `CURRENT-OFFSET = 15` significa que Kafka da
por leídos 0–14 y **no** el 15: esa discrepancia entre lo que el destino sabe y lo que Kafka sabe es la
ventana.

**¿Cuántas veces procesa Kafka ese mensaje? Dos.** Al reiniciar, la primera línea del log fue el `offset=15`
con el mismo `pedido_id=a4d57b68-...` que ya figuraba en Bronze antes del kill. En el caso general son "una o
más": si volviera a caer en la misma ventana habría una tercera entrega. **at-least-once garantiza un piso
(nunca cero), no un techo.**

**Por qué el resultado en Bronze es el mismo.** Porque la segunda entrega entra por `whenMatchedUpdateAll()` y
no por `whenNotMatchedInsertAll()`. Tres mediciones lo confirman, de menos a más independientes de nuestros
conteos.

**1. Aritmética.** Tras el reinicio se procesaron 6 mensajes (offsets 15–20) pero Bronze pasó de 16 a 21
filas: **+5, no +6** — la fila que falta es la del offset 15, que ya existía. Con `.mode("append")` habrían
sido 22 filas con un `pedido_id` repetido.

**2 y 3. La fila es única y fue reescrita**, y **el log de transacciones de Delta** lo confirma sin depender
de nuestros conteos:

```
FILAS con partition=1, offset=15 : 1 · pedido_id: a4d57b68-42a1-4f89-a37d-b06e51f56487
_ingested_at: 2026-08-31T22:38:15.331725 (antes del kill) -> 22:40:41.088812 (tras el reproceso)

|version|operation|insertadas|actualizadas_por_match|
|16     |MERGE    |0         |1                     |
```

Hay una fila, no dos, y el timestamp cambió: el MERGE **sí tocó** la tabla —no ignoró el mensaje— pero la pisó
en vez de duplicarla. Y una sola versión de todo el historial tiene `numTargetRowsInserted = 0` y
`numTargetRowsMatchedUpdated = 1` (las demás son `insertadas=1, actualizadas=0`): esa versión 16 es el
reproceso del offset 15.

**Por qué N ≠ N' y por qué no importa.** La plantilla pide verificar que el conteo antes (N) y después (N')
sea el mismo. **En nuestra corrida N = 16 y N' = 21, así que la comparación literal no se cumple** — y
forzarla sería falsear el dato. Solo daría igual si al reiniciar el consumidor no leyera nada nuevo, y
quedaban 985 pendientes. La pregunta que la prueba realmente responde es **"¿el mensaje reentregado produjo
una fila extra?"**: 6 procesados → +5 filas → `filas_totales − pedido_id_distintos = 0`.

**Por qué no las otras dos garantías.** **at-most-once** sería commitear antes del MERGE: si el proceso muere
en el medio, Kafka ya avanzó el offset y ese pedido no se ingesta nunca — y lo peor no es que se pierda, sino
que **se pierde en silencio**, sin error ni log. **exactly-once** es alcanzable (transacciones + productor
idempotente), pero cuesta coordinación transaccional entre Kafka y el destino y aquí no hace falta: cuando el
destino ya es idempotente por clave natural, at-least-once **es** exactly-once observado desde Bronze.

**El cierre: cuatro interrupciones, cero duplicados.** El SIGKILL fue la única planeada; el pipeline absorbió
cuatro: ese SIGKILL, un corte manual para medir N', la expulsión del consumer group (Pregunta 5c) y un
reinicio para aplicar tuning — más de 1.000 procesamientos sobre un topic de 1.000 mensajes.

```
=== CONTEO BRONZE [VERIFICACION FINAL] ===
  filas_totales = 1000 · pedido_id_distintos = 1000 · duplicados = 0
```

Cada partición con todos sus offsets de 0 a n−1, sin huecos: el MERGE absorbió cada reentrega sin duplicar una
fila.

## Pregunta 2 — Decisión de key

### (a) ¿Qué garantía de orden provee?

`key=region` hace que el particionador por defecto calcule `murmur2(key) % 4`, que es determinista: **una
región siempre cae en la misma partición**. Como Kafka garantiza orden total dentro de una partición, eso da
**orden por región**. Dos evidencias: los 3 mensajes con `key=Bogotá` de la Parte 0 cayeron en P0 con offsets
0, 1 y 2, y en el resumen de los 1.000 mensajes **cada región aparece con una sola partición**. Lo que **no**
garantiza es orden global: un pedido de Bogotá y uno de Medellín viven en particiones distintas (P0 y P1) que
se leen en paralelo.

### (b) ¿Qué problema de balanceo genera?

Una *hot partition*, y salió **peor de lo previsto**: el enunciado anticipa ~40 % para Bogotá; medimos **55,6
%**.

```
=== Resumen: región -> partición -> mensajes ===
  Bogotá P0=403 · Cali P0=153 · Medellín P1=196 · Bucaramanga P1=67 · Barranquilla P3=96 · Otro P2=85
$ GetOffsetShell --topic pedidos-ventas -> 0:556  1:263  2:85  3:96
```

| Partición | Regiones | Mensajes | % | Factor vs P2 |
|---|---|---|---|---|
| **P0** | Bogotá + Cali | **556** | **55,6 %** | **6,5×** |
| P1 | Medellín + Bucaramanga | 263 | 26,3 % | 3,1× |
| P2 | Otro | 85 | 8,5 % | 1,0× |
| P3 | Barranquilla | 96 | 9,6 % | 1,1× |

La causa es una **colisión de hash**: `murmur2("Bogotá") % 4` y `murmur2("Cali") % 4` dan el mismo resultado,
así que P0 carga con las dos. No es mala suerte evitable con código: con 6 regiones y 4 particiones, el
**principio del palomar** obliga a que al menos dos compartan; cuáles, lo decide el hash. **El throughput del
grupo lo fija la partición más cargada**, y no es proyección — lo medimos con el lag total en 571:

| Partición | Mensajes | Procesados | Lag | Estado |
|---|---|---|---|---|
| P1 | 263 | 263 | **0** | terminada |
| P3 | 96 | 96 | **0** | terminada |
| P2 | 85 | 69 | 16 | casi |
| **P0** | **556** | **0** | **556** | **sin empezar** |

Con tres particiones drenadas, **P0 concentraba el 97 % del trabajo restante** (556 de 571): con 4
consumidores, el de P0 tardaría ~6,5 veces más que el de P2.

### (c) ¿Qué clave alternativa usarías?

**`key=pedido_id`.** Cada pedido tiene un UUID distinto, así que `murmur2(uuid) % 4` reparte casi uniforme:
las cuatro particiones cerca de 250 mensajes en vez de 556/263/85/96, y los 4 consumidores terminan a la vez.
El precio es **perder el orden por región**, tolerable aquí porque cada fila es independiente del orden de
llegada.

**Dónde no lo haríamos:** si Bronze guardara una *secuencia de estados* del mismo pedido (creado → pagado →
despachado); ahí procesar "despachado" antes que "pagado" dejaría el estado corrupto y el orden por clave
pasaría de conveniente a obligatorio. Tercera vía: `region + "-" + (hash(pedido_id) % 4)` — 24 claves, Bogotá
repartida entre 4 particiones y afinidad regional conservada (*salting*).

## Pregunta 3 — Número de particiones

### ¿Cuántas particiones lee el consumidor?

**Las 4.** El grupo tiene un solo miembro, así que el coordinador le asigna todas: aparece **el mismo
`CONSUMER-ID` en las cuatro filas**.

```
$ kafka-consumer-groups --describe --group analytics-group
GROUP           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
analytics-group 0          556             556             0    kafka-python-3.0.11-7cc4c579...
analytics-group 1          263             263             0    kafka-python-3.0.11-7cc4c579...
analytics-group 2          85              85              0    kafka-python-3.0.11-7cc4c579...
analytics-group 3          96              96              0    kafka-python-3.0.11-7cc4c579...
```

Un consumidor con las cuatro particiones y lag 0 en todas; Kafka UI lo confirma con `State STABLE`, `Members
1`, `Assigned Partitions 4`, `Total lag 0` (`datos/kafka_ui_lag_cero.png`).

**¿Máximo de consumidores sin que ninguno quede ocioso? 4**, uno por partición. La regla es que **la partición
es la unidad de asignación**: nunca se divide entre dos consumidores del mismo grupo, y la razón no es
arbitraria — si dos leyeran la misma partición en paralelo no habría forma de mantener el orden entre ellos, y
se rompería la garantía de la Pregunta 2(a). Ojo con el matiz de "sin quedar ocioso": con 4 consumidores
ninguno está *sin asignación*, pero por el desbalance medido tres estarían **ociosos la mayor parte del
tiempo** esperando al de P0 (55,6 % de la carga).

**¿Qué pasaría con 6?** Rebalanceo, y quedarían 4 consumidores con una partición y **2 sin ninguna**:
conectados, con heartbeats, pero sin leer un mensaje. **No es un error** —Kafka no lo rechaza— es capacidad
desperdiciada, útil solo como *standby*: si uno de los 4 cae, el rebalanceo entrega su partición a un ocioso.

**Conclusión de diseño:** el número de particiones es el techo del paralelismo de consumo. `--alter` puede
subirlas, pero los mensajes ya escritos no se redistribuyen y —lo importante— al cambiar N cambia el resultado
de `murmur2(key) % N`: una key que iba a P0 con 4 particiones puede ir a P2 con 16, así que **los mensajes
viejos y nuevos de la misma clave quedan separados** y el orden por clave se rompe ahí. Bajar no se puede.

### Qué significa lag = 0, y qué le pasa si detenemos el consumidor

El lag es `LOG-END-OFFSET − CURRENT-OFFSET`: **cuántos mensajes escritos en la partición no ha commiteado el
grupo todavía**. Es deuda pendiente, no velocidad. `lag = 0` significa estar **al día**, no que el topic esté
vacío — la tabla de arriba muestra `CURRENT-OFFSET = LOG-END-OFFSET` con los 1.000 mensajes ahí guardados:
Kafka no borra lo leído, solo avanza el puntero del grupo.

**Al detenerlo, el lag no se congela: el puntero deja de avanzar, pero el `LOG-END-OFFSET` no depende del
consumidor.** Justo tras el SIGKILL el lag saltó a 985 (248 en P1, más 556 + 85 + 96 de las que ni había
empezado). Y ese lag es exactamente **la memoria de dónde retomar**: al reiniciar, el consumidor arrancó en el
offset 15 de P1, el primero no commiteado. Que Kafka retenga los mensajes en el log y el offset en
`__consumer_offsets` es lo que hace que detenerlo sea una pausa y no una pérdida.

## Pregunta 4 — KRaft

### (a) ¿Qué absorbe KRaft?

Todo lo que antes vivía en un servicio de coordinación aparte: **registro de brokers vivos**, **elección de
líder por partición**, **almacenamiento de los metadatos del clúster** y **elección del controller**. El
cambio de fondo no es solo quién hace el trabajo, sino **cómo se guardan los metadatos**: antes eran un árbol
de znodes en un sistema externo; ahora son **un topic interno de Kafka** (`__cluster_metadata`), replicado por
Raft entre los nodos con rol `controller`. Kafka usa su propio log para guardar su propio estado, en vez de
depender de otro sistema con otro punto de falla.

```yaml
KAFKA_PROCESS_ROLES: "broker,controller"        # un solo nodo, los dos roles
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:29093" # el quórum Raft es él mismo
KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"   # listener dedicado al Raft
```

El listener `CONTROLLER` (29093) **no lo toca nunca un productor ni un consumidor**: los clientes hablan por
`EXTERNAL` (9092) y el tráfico interno por `PLAINTEXT` (29092). En producción los roles se separan en nodos
distintos y se listan varios votantes, **3 o 5 — siempre impar**, porque Raft necesita mayoría estricta: con 3
se tolera 1 caída y con 5 se toleran 2, mientras que un par no compra tolerancia extra (4 toleran 1 igual que
3).

### (b) ¿Y si agregáramos coordinación externa?

**Nada — se quedaría corriendo sin que nadie le hable.** Un broker con `KAFKA_PROCESS_ROLES` arranca en modo
KRaft y ya no lee ninguna propiedad de conexión a un coordinador externo; el log lo dice: `Running in KRaft
mode, skipping Zookeeper health check...` — ni siquiera comprueba si hay uno vivo. Y si intentáramos
**conectarlos**, no arranca: los dos modos son **mutuamente excluyentes**. El riesgo real no es técnico sino
**de operación**: dejar en el compose un servicio que aparenta ser parte del sistema y no lo es, de modo que
quien lo lea después diagnosticará mirando el sitio equivocado. Es el síntoma #7 del Troubleshooting del
README.

### (c) ¿Dónde vimos evidencia de KRaft funcionando?

1. **`docker ps` muestra dos contenedores, no tres** — el broker y la UI, que es un cliente.
2. **`kafka-topics --create` funcionó.** Crear un topic es una operación de **metadatos**: va al
controller, que la escribe en el log de metadatos. Que volviera sin error y sin servicio externo prueba que el
controller estaba vivo dentro del broker.
3. **`--describe` reporta `Leader: 1` en las 4 particiones.** Elegir líder era función del
coordinador externo; aquí la hizo el controller integrado.
4. **La más específica: el `CLUSTER_ID` rechazado.**

```
Cluster ID string st1630lab2aKRaftClusterID does not appear to be a valid UUID:
Input string with prefix `st1630lab2aKRaftClusterI` is too long to be decoded as a base64 UUID
```

**Ese error solo existe en modo KRaft**, y ahí está su valor: lo que falla es el `kafka-storage format`, el
paso que **formatea el log de metadatos del quórum Raft dentro del propio broker** antes de arrancar. Ese log
es la estructura que en el modelo anterior no existía —los metadatos vivían fuera—, así que ese preflight
tampoco existía y este error era imposible. Lo resolvimos **sin modificar el archivo del profesor**, generando
un UUID válido y sobreescribiendo solo esa variable en `docker-compose.override.yml`:

```bash
$ docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
OOiHFDxNTKunDxTBeHYiRg   # KRaft exige 22 caracteres (UUID de 128 bits en base64 sin padding); el compose traía 25
```

## Pregunta 5 — Escalabilidad 100×

La magnitud del problema: a las **~2,4 s por mensaje** que logramos tras el tuning, 100.000 mensajes son **~67
horas** solo de consumo (al ritmo previo de ~5 s, ~139 h) — y ambas cuentas subestiman, porque asumen un costo
constante que la sección (c) desmiente.

### (a) Productor: envío asíncrono con callback

Hoy `enviar_pedido()` hace `future.get(timeout=10)` por mensaje: un **round-trip completo cada vez**, con el
productor bloqueado esperando el ack antes de generar el siguiente — a 100.000 mensajes, otras tantas esperas
de red encadenadas. El cambio es `future.add_callback(...)` para acumular el conteo cuando llegue el ack, y un
único `producer.flush()` al final. Así `linger_ms=10` y `batch_size=16384` —que hoy **están configurados pero
no rinden**, porque el `.get()` corta cada lote antes de que se llene— por fin agrupan: un request por lote y
no uno por mensaje. Conviene además subir `batch_size` a ~64 KB y activar `compression_type="lz4"`.

**Lo medimos** en `scripts/benchmark_sync_vs_async.py` (sección 1.5), con los mismos 1.000 pedidos en ambos
modos sobre `pedidos-ventas-benchmark` para no contaminar Bronze:

```
Síncrono   :   16.35 s      61.2 msg/s
Asíncrono  :    0.56 s    1789.0 msg/s
Aceleración:   29.25x
Misma distribución región->partición en ambos modos: True
```

**29,25×**, con el mismo `acks="all"` en los dos. La lectura es que **esos 16 segundos casi no son trabajo**:
son 1.000 latencias de red esperadas de a una. El broker no estaba más ocupado en un modo que en el otro.

El último renglón importa tanto como el tiempo: **la distribución región→partición es idéntica** en ambos
modos, así que el envío asíncrono **no toca la garantía de orden por clave** de la Pregunta 2 — el
particionador se aplica igual, solo cambia cuándo se espera el ack.

Se cede el **manejo de errores**: en síncrono la excepción salta ahí mismo; en asíncrono hace falta
`add_errback(...)`, porque el fallo llega en otro hilo y si nadie lo escucha se pierde en silencio. Lo que
**no** cambiaríamos es `acks="all"`: la garantía del consumidor pierde sentido si el productor puede perder
mensajes antes del log.

### (b) Topic: de 4 a ~16 particiones

4 particiones son el techo del paralelismo (Pregunta 3). Subir a **16** permite 16 consumidores; el número
sale de que a ~2,4 s por mensaje una partición de ~6.250 mensajes tarda ~4 h, un lote nocturno viable. **La
trampa: más particiones no arreglan el desbalance por sí solas.** Con `key=region` y solo **6 regiones
distintas**, 16 particiones dejarían **10 vacías** y Bogotá seguiría concentrando su 40 % en una sola. La
regla: **el techo real no es el número de particiones sino el de claves distintas** — `min(particiones,
claves)` acota el paralelismo efectivo. El cambio solo sirve acompañado del *salting* de la Pregunta 2(c), y
hay que hacerlo **antes** de tener tráfico serio: `--alter` no redistribuye lo escrito y cambia el destino de
las claves existentes.

### (c) Consumer group: procesar por lotes

El cuello de botella real. Cada mensaje dispara su propio MERGE, y Delta registra:

```
executionTimeMs : 4.500 – 5.600 ms   scanTimeMs : ~2.000 ms
numTargetBytesAdded : ~4.800 bytes (una sola fila)
```

Lo que hay que mirar es la relación entre `scanTimeMs` y `numTargetBytesAdded`: **~2 segundos buscando para
escribir 4,8 KB**. El tiempo no se va en escribir sino en **escanear el destino para buscar el match**, y ese
costo es casi el mismo para 1 fila que para 5.000.

**Y no es constante: crece.** De ~5 s por mensaje con ~15 filas en Bronze a **~20 s** con 124 filas. La causa
es el problema de ***small files***: cada MERGE de una fila escribe su propio Parquet.

```
$ find /lake/bronze/pedidos -name '*.parquet' | wc -l -> 124   # 124 archivos para 124 filas
$ du -sh /lake/bronze/pedidos                         -> 3.0M  # ~4,8 KB por archivo
```

Como el MERGE escanea **todos** los archivos existentes para buscar el `pedido_id`, el costo del mensaje `n`
es proporcional a `n` y el total es **cuadrático, no lineal**: los 1.000 mensajes del lab habrían tardado más
de **5 horas** y subiendo.

**El diseño ya se rompió con solo 1.000 mensajes.** A mitad del consumo empezaron a fallar todos los commits,
10 veces consecutivas (offsets 44–53):

```
[ERROR] offset=44 partition=1 no se commiteó -- se reprocesará. Causa:
CommitFailedError: ... the consumer was kicked out of the group.
```

La cadena: `kafka-python` trae hasta `max_poll_records` (500 por defecto) en un `poll()` y no vuelve a
llamarlo hasta procesarlos todos; a ~5 s por MERGE son **~40 minutos entre llamadas**, contra los **300 s** de
`max_poll_interval_ms`. Para el broker, un consumidor que no vuelve a hacer `poll()` está muerto: lo expulsa y
dispara un rebalanceo, y desde ahí ningún commit se acepta porque ya no es dueño de esas particiones
(`datos/consumidor_expulsion_evidencia.txt`). Lo notable: **durante esos 10 errores no se perdió ni se duplicó
un dato** — el MERGE idempotente absorbió los reprocesos.

**El cambio:** consumir con `poll(max_records=5000)`, construir **un solo DataFrame**, hacer **un** MERGE y
recién ahí `commit()`. El escaneo se amortiza: pasar de 100.000 MERGE de ~5 s a **20 MERGE** (100.000 ÷ 5.000)
de quizá 30 s cambia el orden de magnitud del pipeline, y como cada MERGE escribe un archivo con 5.000 filas
el problema de *small files* desaparece por construcción. **Resuelve los dos fallos a la vez**: con lotes, el
tiempo entre `poll()` deja de crecer con el volumen.

**Por qué no debilita la garantía.** El commit **sigue yendo después del MERGE**, así que sigue siendo
at-least-once: la ventana crítica está en el mismo sitio. Lo único que cambia es **la unidad de reproceso** —
si el proceso cae, se reentrega el lote entero en vez de un mensaje. Y eso es seguro por lo mismo que probamos
en la Pregunta 1: el MERGE es idempotente por `pedido_id`, así que reprocesar 5.000 filas de las cuales 4.999
ya estaban da el mismo resultado que reprocesar una.

**El parche vs. la solución de fondo.** Para terminar el lab aplicamos:

```python
max_poll_records=1                                              # evita la expulsión
.config("spark.sql.shuffle.partitions", "4")                    # default: 200
.config("spark.databricks.delta.optimizeWrite.enabled", "true")
.config("spark.databricks.delta.autoCompact.enabled", "true")
# + datalake movido de bind mount de Windows a volumen Docker nativo
```

De ~20 s a **~2,4 s por mensaje**, sin degradación progresiva (947 líneas `[OK]` y 0 errores en la corrida
final). Pero eso **compró tiempo, no arregló el diseño**: `max_poll_records=1` evita el timeout a costa de un
round-trip por mensaje, y `autoCompact` limpia archivos pequeños **después** de crearlos. **Dato
transversal:** ninguno de los tres tropiezos provocó pérdida ni duplicación — el pipeline degradó su
throughput, nunca su corrección, y lo podemos afirmar porque lo medimos.

## Anexo — Parte 0: exploración antes de escribir código

### 1. Creación del topic y factor de replicación

```bash
docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas \
  --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092   # -> Created topic pedidos-ventas.
```

**Por qué el factor es 1 en local:** hay un solo broker. El factor de replicación es *en cuántos brokers
distintos vive cada partición*, así que pedir 2 con un único nodo es imposible — Kafka lo rechazaría con
`InvalidReplicationFactorException`.

**En producción:** el estándar es **replicación 3 con `min.insync.replicas=2`**. Tres copias permiten perder
un nodo sin perder datos ni disponibilidad de escritura, y `min.insync.replicas=2` obliga a que dos réplicas
confirmen antes de dar por escrito el mensaje; si solo queda una viva, el broker **rechaza** la escritura en
vez de aceptarla sin respaldo.

**Y esto es lo que le da sentido a nuestro `acks="all"`**, que significa "espera a que todos los ISR
confirmen": aquí el ISR tiene un solo miembro, el propio líder, así que **`acks="all"` es operativamente
idéntico a `acks=1`**. Está puesto correctamente para producción, pero en este clúster no hay dónde demostrar
su efecto.

### 2. El otro topic: `__consumer_offsets`

**Un matiz que encontramos:** justo después de crear `pedidos-ventas`, el `--list` devolvió **solo**
`pedidos-ventas`, ni siquiera pidiendo los internos con `--exclude-internal false`. El enunciado da por
sentado que aparece; en nuestra corrida no. Kafka lo crea **de forma perezosa**, la primera vez que un
consumer group commitea un offset — y no habíamos corrido ningún consumidor. Solo después de
`analytics-group`:

```
$ kafka-topics --list  ->  __consumer_offsets, pedidos-ventas
$ kafka-topics --describe --topic __consumer_offsets
PartitionCount: 50   ReplicationFactor: 1
Configs: compression.type=producer,cleanup.policy=compact,segment.bytes=104857600
```

**Para qué sirve:** guarda de forma durable hasta qué offset ha leído cada `(grupo, topic, partición)` —
exactamente lo que escribe `consumer.commit()`. Que sea un topic normal y no una estructura aparte es lo
elegante: los offsets se replican con el mismo mecanismo que los datos.

Dos detalles del `--describe`: **50 particiones**, para que cada grupo se mapee a una por hash de su nombre y
miles de grupos no se peleen por la misma; y **`cleanup.policy=compact`** en vez de `delete`, porque la
compactación conserva solo el **último** valor de cada clave — no importa que el grupo iba por el 3 y luego el
4, sino dónde está ahora, y con `delete` un grupo inactivo perdería su posición al truncarse por tiempo. Esto
conecta con la idempotencia: el valor guardado para `(analytics-group, pedidos-ventas, 1)` era **15** cuando
matamos el proceso, y por eso Kafka reentregó ese mensaje (Pregunta 1).

### 3. Kafka UI — particiones del topic

```
Topic: pedidos-ventas   TopicId: X13LgXF6Qt2D5kds7sKhiw   PartitionCount: 4   ReplicationFactor: 1
    Partition: 0    Leader: 1   Replicas: 1   Isr: 1
    Partition: 1    Leader: 1   Replicas: 1   Isr: 1
    Partition: 2    Leader: 1   Replicas: 1   Isr: 1
    Partition: 3    Leader: 1   Replicas: 1   Isr: 1
```

**4 particiones, numeradas 0 a 3**, como en la columna `_kafka_partition` de Bronze. **El líder es el broker 1
en las cuatro**, porque es el único que hay (`KAFKA_NODE_ID: 1`). `Replicas` lista los brokers con copia de
esa partición e `Isr` (*in-sync replicas*) cuáles están al día con el líder; aquí ambas valen 1: la única
réplica es el nodo 1, en sincronía consigo misma. Kafka UI lo reporta como **In Sync Replicas 4 of 4**, con
**Segment Count 4** y **Message Count 1000** (`datos/kafka_ui_particiones.png`).

**En un clúster real** el liderazgo se reparte a propósito, porque **todo el tráfico de lectura y escritura de
una partición pasa por su líder** — las seguidoras solo replican. Si las 4 tuvieran líder en el mismo nodo,
ese nodo absorbería el 100 % del tráfico del topic. Y cuando un broker cae, el controller promueve a alguna
réplica de su ISR: por eso `min.insync.replicas` es la otra mitad de `acks="all"`.

### 4. Tres mensajes con la misma key

```
Partition:0 | Offset:0 | Bogotá | {"nota":"prueba 1"}
Partition:0 | Offset:1 | Bogotá | {"nota":"prueba 2"}
Partition:0 | Offset:2 | Bogotá | {"nota":"prueba 3"}
```

Los tres en **P0**, con offsets **consecutivos**. **¿Podrían haber aparecido en particiones distintas? No.**
El particionador por defecto aplica `murmur2(key) % n_particiones`, una función **determinista**: misma key y
mismo número de particiones ⇒ misma partición, siempre, desde cualquier productor y sin aleatoriedad de por
medio.

Solo cambiaría si cambiara el **número de particiones** (por eso `--alter` rompe el orden por clave, Pregunta
3), con un **particionador personalizado**, o si la key llegara con bytes distintos (`"Bogotá"` en UTF-8 vs.
Latin-1: `murmur2` hashea bytes, no caracteres). Los offsets 0, 1, 2 son la garantía de orden de la Pregunta
2(a) en su forma más pequeña.

### 5. Mensajes sin key

Primer envío, dos mensajes en una misma sesión del `console-producer`:

```
Partition:0 | Offset:3 | null | {"nota":"sin key A"}
Partition:0 | Offset:4 | null | {"nota":"sin key B"}
$ GetOffsetShell -> 0:5  1:0  2:0  3:0
$ GetOffsetShell (tras repetir 4 veces en invocaciones separadas) -> 0:6 (+1)  1:1 (+1)  2:2 (+2)  3:0 (+0)
```

Los dos primeros fueron a P0 — que a primera vista parece **el mismo comportamiento que con key fija**, y por
eso dudamos: con dos mensajes en la misma partición no se puede distinguir "el hash siempre da 0" de
"coincidieron". Al repetir el envío cuatro veces más, cada una en una invocación separada, los 4 mensajes se
repartieron entre **P0, P1 y P2**. **¿Es el mismo comportamiento? No, y la diferencia es de naturaleza, no de
grado.** Con key, el destino es función determinista de la key. Sin key, `murmur2` no tiene nada que hashear y
Kafka usa el **sticky partitioner**: elige una partición al azar y "se pega" a ella mientras llena un lote, y
cambia recién cuando el lote se cierra o termina la sesión. Eso explica lo que vimos: los dos primeros,
enviados juntos, compartieron el lote pegajoso; al repetir en sesiones separadas, cada una eligió de nuevo.

**La consecuencia de diseño: sin key no hay ninguna garantía de orden**, ni siquiera entre dos mensajes
enviados uno detrás del otro, porque pueden acabar en particiones que se leen en paralelo — la apariencia de
agrupamiento del sticky partitioner es un efecto del batching, no una garantía. Lo que se gana es el mejor
balanceo posible. Es el trade-off de la Pregunta 2 llevado al extremo: con key, orden sin balanceo; sin key,
balanceo sin orden.

### 6. Nota de método: por qué recreamos el topic

Los 9 mensajes de prueba de esta Parte 0 son `{"nota": "..."}` y **no tienen `pedido_id`**, así que habrían
hecho fallar el `MERGE ... ON pedido_id` y —peor— habrían ensuciado los conteos de la prueba de idempotencia,
donde toda la evidencia está en la diferencia entre 21 y 22 filas. Por eso borramos y recreamos el topic vacío
antes de correr el productor (`GetOffsetShell` confirmó `0:0, 1:0, 2:0, 3:0`).

Dejarlos habría revelado además una limitación del script: `consumer.commit()` sin argumentos commitea la
**posición** del consumidor, no el offset del mensaje recién procesado, así que el commit del **siguiente**
mensaje exitoso avanza por encima de uno fallido y este nunca se reintenta — el modo de falla que criticamos
de at-most-once en la Pregunta 1. En producción haría falta `commit({tp: OffsetAndMetadata(msg.offset + 1,
None)})` o una *dead letter queue*.
