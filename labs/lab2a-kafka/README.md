# Lab 2a · Productor/consumidor Kafka

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Última edición:** 2026-08-24
**Peso:** parte del 30% de laboratorios (ver `../../docs/evaluacion.md`)
**Tiempo estimado:** 6-8 horas, distribuidas en 3 días
**Entrega:** Pull Request antes del inicio de S7

> **Este es un laboratorio 100% en casa.** Lo haces de forma autónoma,
> sin sesión de clase — esta guía debe bastarte por sí sola. Si te
> atoras, revisa primero la sección de Troubleshooting antes de
> escribir al profesor.

## Objetivo

Implementar el flujo completo de streaming con Kafka: un **productor**
que publica eventos de pedidos en el topic `pedidos-ventas`, y un
**consumidor** que los lee e ingesta en Bronze del datalake con
garantía **at-least-once** real, usando el mismo patrón de `MERGE`
Delta idempotente que construiste en el Lab 1b.

## Prerequisito y verificación

**Conceptual:** debes tener fresco el patrón `MERGE` Delta del Lab 1b
(`../lab1b-batch/scripts/02_silver.py`, Parte 3.7) — este lab lo
reutiliza tal cual, solo que ahora lo dispara un mensaje de Kafka en
vez de un batch de CSV.

**De software**, necesitas:

```bash
docker --version && docker compose version   # o docker-compose --version
python3 -c "import pyspark, delta; print('pyspark/delta OK')"
pip install kafka-python   # si no lo tienes
```

**Levanta la infraestructura de este lab** (no necesitas ningún
recurso del Lab 1b en AWS — este lab corre 100% local):

```bash
cd labs/lab2a-kafka
docker-compose up -d
```

Espera ~30 segundos (el healthcheck del broker tarda en pasar) y
verifica:

```bash
docker ps | grep kafka
```

**Output esperado:** dos contenedores, `st1630-lab2a-kafka` y
`st1630-lab2a-kafka-ui`, ambos con estado `Up`.

```bash
docker exec st1630-lab2a-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Output esperado:** una lista **vacía** (sin error) — todavía no has
creado ningún topic; eso es exactamente la Parte 0 de este lab.

Si el broker no responde, ve a Troubleshooting antes de seguir.

## Arquitectura del lab

```
  [Generador de pedidos]
          │
          ▼
  [Producer] ── key=region ──┐
          │                   │  push (acks=all)
          ▼                   ▼
  ┌─────────────────────────────────────────┐
  │  Kafka Cluster — KRaft (1 broker)        │
  │  Topic: pedidos-ventas (4 particiones)   │
  │   P0            P1            P2    P3   │
  │   Bogotá ~40%   Medellín ~20% Cali  Resto│
  │                                 ~15% ~25%│
  └─────────────────────────────────────────┘
          │ pull
          ▼
  [Consumer — analytics-group]
  enable_auto_commit=False
  ┌─────────────────────────────────┐
  │ 1. procesar mensaje              │
  │ 2. MERGE Delta a Bronze          │──▶ idempotente por pedido_id
  │ 3. SI el MERGE fue OK: commit    │    (Lab 1b, mismo patrón)
  │    SI falló: NO commit, reintenta│
  └─────────────────────────────────┘
          │
          ▼
  [Bronze — /tmp/lake/bronze/pedidos]
  (BRONZE_PATH configurable por env var → S3 en producción)
```

**Por qué el orden de los pasos 2-3 del consumidor es lo que hace este
pipeline at-least-once y no at-most-once:** commitear ANTES de
procesar arriesga perder el mensaje si el consumidor cae en el medio;
commitear DESPUÉS de un MERGE exitoso garantiza que, si algo falla,
Kafka vuelve a entregar ese mensaje — y como el MERGE es idempotente
por `pedido_id`, reprocesarlo no produce ningún duplicado en Bronze.
Esa es la conexión completa entre streaming (Kafka) y batch (Delta
Lake) que este lab construye.

## Parte 0 — Exploración antes de escribir código (Día 1, 30 min)

**Obligatoria antes de escribir una sola línea de código.** Ejecuta
estos comandos y documenta lo que observes en `kafka_design.md` (copia
`plantillas/kafka_design_template.md`) — no son las 5 preguntas de
diseño de la Parte 3, son un anexo de exploración que también forma
parte de ese mismo documento y de su 25% de la nota total del
`kafka_design.md` (ver Rúbrica).

1. **Crea el topic** `pedidos-ventas` con 4 particiones y factor de
   replicación 1:

   ```bash
   docker exec st1630-lab2a-kafka kafka-topics --create \
     --topic pedidos-ventas --partitions 4 --replication-factor 1 \
     --bootstrap-server localhost:9092
   ```

   Anota el comando exacto que usaste. ¿Por qué el factor es 1 en
   local? ¿Por qué sería distinto (piensa en el estándar visto en
   clase) en producción?

2. **Lista los topics** de nuevo:

   ```bash
   docker exec st1630-lab2a-kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

   Además de `pedidos-ventas`, ¿qué otro topic aparece? No lo creaste
   tú a propósito — ¿para qué crees que sirve? (pista: tiene que ver
   con lo que tu consumer group va a necesitar guardar en la Parte 2).

3. **Abre Kafka UI** en [http://localhost:8080](http://localhost:8080).
   Navega a **Topics → pedidos-ventas → Partitions**. Describe qué ves:
   ¿cuántas particiones? ¿quién es el líder de cada una?

4. **Envía 3 mensajes de prueba** con la misma key:

   ```bash
   docker exec -it st1630-lab2a-kafka kafka-console-producer \
     --topic pedidos-ventas --bootstrap-server localhost:9092 \
     --property "parse.key=true" --property "key.separator=:"
   ```

   Y escribe (una línea por mensaje, Ctrl+D para salir):
   ```
   Bogotá:{"nota":"prueba 1"}
   Bogotá:{"nota":"prueba 2"}
   Bogotá:{"nota":"prueba 3"}
   ```

   Revisa en Kafka UI en qué partición aparecieron los 3. ¿Podrían
   haber aparecido en particiones distintas? ¿Por qué sí o por qué no?

5. **Envía 2 mensajes sin key** (deja `key.separator` pero no escribas
   nada antes de los `:`, o usa `kafka-console-producer` sin la
   propiedad `parse.key`). ¿En qué partición(es) aparecen? Repite el
   envío un par de veces — ¿es siempre el mismo comportamiento que con
   key fija?

## Parte 1 — Productor (Día 1, 2 horas)

Abre `scripts/productor_kafka.py` y completa los TODO marcados —
lee primero el docstring del archivo, te dice exactamente qué está
dado y qué es tuyo.

### 1.1 Configuración del productor

El TODO 1.1 te pide construir el `KafkaProducer` con
`bootstrap_servers`, los serializers y `acks='all'`. El comentario
justo antes del TODO explica cada parámetro — léelo antes de escribir
código, no lo saltes.

### 1.2 Generador de pedidos sintéticos

Ya está resuelto (no hay decisión de diseño en generar datos de
prueba) — genera 1.000 pedidos con la distribución de regiones,
categorías, canales y métodos de pago especificada en el propio script.

### 1.3 Envío con key=region

El TODO 1.3 es la decisión de diseño central del productor: por qué
`key=region` y no `key=pedido_id`. El comentario del script te explica
el trade-off orden-vs-balanceo — tu trabajo es implementar el envío
síncrono (`producer.send(...)` + `future.get(...)`) usando esa key.

**Verifica** al correr el script: el resumen final (región → partición
→ cantidad) te muestra si tu implementación realmente está mandando
cada región siempre a la misma partición. Guarda ese resumen — lo vas
a necesitar para la Pregunta 2 de `kafka_design.md`.

### 1.4 Logging y verificación

El TODO 1.4 (dentro del mismo loop de 1.3) te pide acumular el conteo
región → partición en el diccionario que el script ya declara, para
que el resumen final tenga datos reales.

### 1.5 Variante para quien termina antes (opcional)

Si te sobra tiempo: implementa una segunda versión del envío en modo
**asíncrono**, usando el callback que expone `future.add_callback(...)`
en vez de bloquear con `future.get()` en cada mensaje. Mide cuánto
tarda enviar los 1.000 mensajes en cada modo (`time.time()` antes y
después del loop) y compara. ¿En qué escenario usarías cada uno? No es
obligatorio para la rúbrica, pero es una buena forma de sentir en carne
propia la diferencia de throughput.

**Corre el productor** cuando termines los TODO:

```bash
cd labs/lab2a-kafka/scripts
python3 productor_kafka.py
```

## Parte 2 — Consumidor (Día 2, 3 horas)

Abre `scripts/consumidor_kafka.py`. Esta es la parte central del lab
— lee el docstring completo antes de empezar.

### 2.1 Configuración del consumidor

El TODO 2.1 te pide construir el `KafkaConsumer`. El comentario antes
del TODO explica cada parámetro en detalle, especialmente
`enable_auto_commit=False` — es la decisión más importante del script,
léela dos veces si hace falta.

### 2.2 / 2.3 Lógica de procesamiento y el MERGE Delta

- `construir_fila_bronze()` (TODO): agrega las 4 columnas de
  trazabilidad (`_kafka_offset`, `_kafka_partition`, `_kafka_topic`,
  `_ingested_at`) a cada mensaje antes de escribirlo. Este patrón —
  poder rastrear de qué offset/partición/topic vino cada fila de
  Bronze — es exactamente lo que necesitarías en producción para
  auditar o reprocesar selectivamente.
- `merge_a_bronze()` (dado): el mismo `MERGE ... ON pedido_id` del Lab
  1b, adaptado a un solo mensaje a la vez en vez de un batch. Lee el
  comentario completo — explica por qué es idempotente y por qué eso
  es justo lo que hace posible usar at-least-once.
- El TODO del `main()` (2.2/2.3 combinado): completa el `try/except`
  que procesa cada mensaje, hace el MERGE, y **solo si no hubo
  excepción** commitea el offset. Si hay excepción: no commitear,
  loggear el offset que falló.

### 2.4 Prueba de idempotencia — OBLIGATORIA

Sigue los 5 pasos exactos de `plantillas/prueba_idempotencia_template.md`
y documenta la evidencia real (logs + conteos de Bronze antes/después)
en tu `datos/prueba_idempotencia.md`. **Sin esta evidencia el lab es
parcial** — no basta con describir la prueba, tienes que haberla
corrido y mostrar los números.

### 2.5 Verificación del offset en Kafka UI

1. Ve a [http://localhost:8080](http://localhost:8080) → **Consumer
   Groups → analytics-group**.
2. Mientras el consumidor procesa, refresca y observa cómo el **lag**
   (mensajes pendientes de esa partición) va bajando.
3. En tu `kafka_design.md` (o donde documentes evidencia), explica con
   tus palabras qué significa `lag = 0`, y qué le pasa al lag si
   detienes el consumidor a mitad de proceso.
4. Captura una imagen del Consumer Group con `lag = 0` y guárdala como
   `datos/kafka_ui_lag_cero.png` — es parte del entregable.

**Corre el consumidor** cuando termines los TODO:

```bash
cd labs/lab2a-kafka/scripts
python3 consumidor_kafka.py
```

## Parte 3 — kafka_design.md (Día 3, 2 horas)

El entregable diferenciador del lab. Copia
`plantillas/kafka_design_template.md` a tu carpeta de entrega y
responde las 5 preguntas citando evidencia concreta de tu propio
pipeline (tus logs, tu resumen región→partición, tus capturas de Kafka
UI) — una respuesta que solo repite la teoría de clase sin conectarla
con tu ejecución no cuenta como completa:

1. Garantía elegida (at-least-once) — qué pasa si el consumidor falla
   entre el MERGE y el commit, y por qué el resultado en Bronze es el
   mismo de todas formas.
2. Decisión de key (`region`) — garantía de orden, problema de
   balanceo, alternativa si el orden no importara.
3. Número de particiones — cuántas lee tu consumidor, máximo de
   consumidores sin ociosidad, qué pasa con 6.
4. KRaft — qué reemplaza, qué pasaría si agregaras un segundo servicio
   de coordinación, cuándo viste evidencia de que funciona.
5. Escalabilidad 100× — un cambio en productor, uno en el topic, uno
   en el consumer group.

## Entregable — estructura del PR

```
labs/lab2a-kafka/
└── <tu-nombre>/
    ├── scripts/
    │   ├── productor_kafka.py
    │   └── consumidor_kafka.py
    ├── datos/
    │   ├── prueba_idempotencia.md     # con logs/capturas reales
    │   └── kafka_ui_lag_cero.png      # captura del Consumer Group
    ├── kafka_design.md                # las 5 preguntas + Parte 0
    ├── bitacora_delegacion.md
    └── README.md                      # tu nombre, fecha, config de tu cluster
```

Abre el PR hacia `main` con título
`lab(2a): <tu nombre> — productor/consumidor Kafka`, antes del inicio
de S7.

## Rúbrica de evaluación

| Criterio | Peso | Completo | Parcial | Incompleto |
|---|---|---|---|---|
| **Productor** | 25% | `key=region`, `acks='all'`, 1.000 mensajes enviados, log de partición por región | Funciona pero sin key o sin `acks='all'` | No existe o no conecta |
| **Consumidor at-least-once** | 30% | `enable_auto_commit=False`, commit manual DESPUÉS del MERGE, columnas de trazabilidad `_kafka_*` | Auto-commit activado o sin MERGE (usa `append`) | No existe |
| **Prueba de idempotencia** | 20% | Documentada con evidencia real de que el mismo mensaje procesado dos veces no duplica en Bronze | Se menciona pero sin evidencia real (logs/conteos) | No se hizo |
| **`kafka_design.md`** | 25% | Las 5 preguntas (+ Parte 0) respondidas con argumentos técnicos específicos a TU pipeline (cita tus offsets, tu MERGE, ISR, KRaft, tu propia hot partition) | 3-4 preguntas respondidas o respuestas genéricas que repiten la teoría sin conectar con tu ejecución | Menos de 3 preguntas o sin ningún argumento técnico |

## Bitácora de delegación

Este lab sigue `../../docs/politica-ia.md`.

| Tarea | ¿Se puede delegar? | Justificación |
|---|---|---|
| Sintaxis de `kafka-python`/PySpark (dudas puntuales) | Sí | Bajo valor de aprendizaje memorizar sintaxis |
| Generador de datos sintéticos (ya dado) | N/A | No hay decisión de diseño ahí — ya viene resuelto |
| Boilerplate del `docker-compose.yml` (ya dado) | N/A | Infraestructura estándar de KRaft — ya viene resuelto, no lo modifiques |
| El `MERGE` Delta de `merge_a_bronze()` (ya dado) | N/A | Ya lo construiste tú mismo en el Lab 1b; aquí solo se reutiliza adaptado a un mensaje |
| Decidir `key=region` y justificarla | **No** | Es la decisión de diseño central del productor |
| `enable_auto_commit=False` + la coreografía commit-después-del-MERGE | **No** | Es el objetivo 3 de la sesión — si un agente te lo resuelve, no vas a poder explicar la prueba de idempotencia |
| Ejecutar y documentar la prueba de idempotencia | **No** | Si no la corriste tú, no tienes evidencia real que citar |
| `kafka_design.md` (las 5 preguntas + Parte 0) | **No** | Es el entregable central del lab |
| Decidir el número de particiones al crear el topic | **No** | Conecta directo con la regla "N particiones = N consumidores máximos activos" (Pregunta 3) |

## Troubleshooting

| # | Error / síntoma | Causa probable | Solución |
|---|---|---|---|
| 1 | `NoBrokersAvailable` al conectar | El broker no está corriendo o todavía no pasó el healthcheck | `docker ps` — confirma que `st1630-lab2a-kafka` diga `Up (healthy)`. Si no: `docker-compose up -d && sleep 30` y reintenta |
| 2 | `UnknownTopicOrPartitionError` / `KafkaTimeoutError` al producir | El topic `pedidos-ventas` no existe todavía (recuerda: `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`) | Créalo a mano (Parte 0, paso 1): `docker exec st1630-lab2a-kafka kafka-topics --create --topic pedidos-ventas --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092` |
| 3 | El consumidor no recibe ningún mensaje (pero tampoco da error) | `auto_offset_reset="latest"` en vez de `"earliest"`, y el productor ya había enviado antes de que el consumidor arrancara | Usa `auto_offset_reset="earliest"` en el TODO 2.1, o si ya corriste el consumidor una vez con `group_id="analytics-group"`, Kafka ya tiene un offset guardado para ese grupo — usa un `group_id` nuevo para forzar releer desde el principio |
| 4 | El consumidor recibe mensajes pero Bronze no crece | El MERGE está fallando dentro del `try/except` (por eso nunca hace commit) | Revisa el mensaje de `[ERROR]` que tu propio `except` debería estar imprimiendo — normalmente es un schema mismatch entre las columnas del mensaje JSON y lo que Delta espera |
| 5 | Duplicados en Bronze después de reiniciar el consumidor | La condición del MERGE no es `pedido_id`, o estás usando `.mode("append")` en vez de `merge()` | Revisa `merge_a_bronze()`: la condición debe ser `existente.pedido_id = nuevo.pedido_id`. Si de verdad usaste `append`, los duplicados son el síntoma esperado de at-most-once — no de at-least-once |
| 6 | `NotLeaderForPartitionError` intermitente | El líder de una partición cambió y el cliente tenía metadata desactualizada (más común en clústers multi-broker; con 1 solo broker local es raro pero puede pasar tras un restart) | `kafka-python` reintenta automáticamente. Si persiste, reinicia el script — pedirá metadata fresca al arrancar |
| 7 | Buscas un contenedor o config de un servicio de coordinación externo y no existe | Este lab usa KRaft — la coordinación vive dentro del propio broker, no hay ningún servicio adicional que levantar ni al que apuntar | Elimina cualquier configuración de coordinación externa de tu código; usa solo `bootstrap_servers=['localhost:9092']` |
| 8 | `SparkSession` no puede leer/escribir Delta | Falta el paquete Delta al lanzar `python3` directo (sin `spark-submit --packages`) | Asegúrate de tener `pyspark` y `delta-spark` instalados (`pip install pyspark delta-spark`) — la config de `.config("spark.sql.extensions", ...)` ya está en el script, pero si sigue fallando corre con `PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_3.5_2.12:3.1.0 pyspark-shell"` |
| 9 | El lag en Kafka UI nunca baja a cero | `enable_auto_commit=False` está bien puesto, pero `consumer.commit()` nunca se ejecuta (olvidaste llamarlo, o el `try` siempre lanza excepción antes de llegar ahí) | Verifica que veas logs `[OK]` en tu terminal (no solo `[ERROR]`) — sin al menos un commit exitoso, Kafka nunca actualiza lo que considera "leído" para tu group |

## Referencias

- [Documentación oficial de Apache Kafka](https://kafka.apache.org/documentation/) — particiones, consumer groups, KRaft.
- [Documentación de Delta Lake](https://docs.delta.io/) — `MERGE INTO`, idempotencia.
- [kafka-python](https://kafka-python.readthedocs.io/) — la librería cliente usada en este lab.
- `../lab1b-batch/README.md` y `../lab1b-batch/scripts/02_silver.py` — el patrón MERGE Delta que este lab reutiliza.
- Slides de la clase S6 del curso (arquitectura de Kafka, garantías de entrega, KRaft).
