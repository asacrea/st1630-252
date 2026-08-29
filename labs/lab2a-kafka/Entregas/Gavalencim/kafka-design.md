# Diseño Kafka — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 29/08/2026

## Pregunta 1 — Garantía elegida

Justificar la elección de *at-least-once* en términos del modelo de commit de offset y la idempotencia del MERGE Delta. ¿Qué ocurre si el consumidor falla después del MERGE pero antes del commit? ¿Cuántas veces procesará Kafka ese mensaje? ¿Por qué el resultado en Bronze es el mismo?

**Respuesta:**

Se eligió la garantía *at-least-once* porque el consumidor ejecuta primero el `MERGE` en Bronze y solo después realiza el `commit` del offset en Kafka. Si el proceso falla después del `MERGE` pero antes del `commit`, Kafka puede volver a entregar ese mismo mensaje cuando el consumidor se reinicia. En ese escenario, el mensaje puede procesarse más de una vez desde la perspectiva de Kafka, pero el resultado en Bronze permanece idéntico, ya que la escritura se realiza mediante `MERGE ... ON pedido_id`, lo cual evita duplicados.

Esto se verificó de forma consistente en la prueba de idempotencia: antes de reiniciar el consumidor, el conteo en Bronze fue `N = 1000`; después del reinicio, el conteo se mantuvo en `N' = 1000`. Como `N = N'`, el reinicio no generó nuevas filas en Bronze, lo que confirma que el `MERGE` fue idempotente pese a que el modelo de entrega de Kafka permite el reprocesamiento de mensajes.

---

## Pregunta 2 — Decisión de key

**(a)** ¿Qué garantía de orden provee la clave `region`?

**(b)** ¿Qué problema de balanceo genera, dado que Bogotá concentra ~40% del tráfico en un topic de 4 particiones?

**(c)** ¿Qué clave alternativa sería preferible si el orden no importara pero el balanceo fuera crítico?

**Respuesta:**

**(a) Garantía de orden.** Al usar `key=region`, la garantía de orden se limita a cada partición individual: los mensajes que comparten la misma clave tienden a dirigirse a la misma partición, y dentro de ella Kafka preserva su orden relativo. En este laboratorio, eso permite mantener el orden de los eventos por región, lo cual sería útil para un análisis posterior de la secuencia de eventos regionales.

**(b) Problema de balanceo.** Esta decisión genera desbalanceo cuando una región concentra mucho más tráfico que las demás. El resumen final del productor mostró la siguiente distribución:

| Región | Partición | Mensajes |
|---|---|---|
| Bogotá | P0 | 368 |
| Medellín | P1 | 202 |
| Cali | P0 | 161 |
| Barranquilla | P3 | 106 |
| Bucaramanga | P1 | 100 |
| Otro | P2 | 63 |

Estos datos evidencian que Bogotá, por su mayor volumen, sobrecargó una sola partición, y que Cali también fue asignada a `P0`, lo que hizo que esa partición recibiera notablemente más mensajes que las demás (una *hot partition*).

**(c) Clave alternativa.** Si el orden por región no fuera relevante pero el balanceo sí fuera crítico, sería preferible usar una clave de mayor cardinalidad, como `pedido_id` o una combinación más distribuida. Esto repartiría los mensajes de forma más uniforme entre particiones y evitaría *hot partitions* como la observada en `P0`, a costa de perder el orden agrupado por región.

---

## Pregunta 3 — Número de particiones

El topic tiene 4 particiones y el consumer group tiene 1 consumidor. ¿Cuántas particiones lee ese consumidor? ¿Cuál es el máximo de consumidores activos sin que ninguno quede ocioso? ¿Qué pasaría si se añadieran 6?

**Respuesta:**

Dado que el topic tiene 4 particiones y el consumer group tuvo un único consumidor activo, ese consumidor leyó las 4 particiones asignadas. Esto se confirmó en Kafka UI, donde el grupo `analytics-group` aparecía con `Assigned Partitions: 4` y `Members: 1`.

El máximo de consumidores activos que pueden añadirse sin que ninguno quede ocioso es **4**, ya que dentro de un mismo consumer group cada partición solo puede ser leída por un consumidor a la vez. Si se añadieran 6 consumidores al mismo grupo, únicamente 4 quedarían trabajando y los 2 restantes permanecerían ociosos, dado que no existirían particiones adicionales para asignarles.

---

## Pregunta 4 — KRaft

**(a)** ¿Qué función asume KRaft que antes dependía de un servicio de coordinación externo?

**(b)** ¿Qué ocurriría al intentar agregar un servicio de coordinación externo adicional al `docker-compose.yml`?

**(c)** ¿En qué momento del lab se evidenció que KRaft estaba funcionando?

**Respuesta:**

**(a)** KRaft asume internamente la función de coordinación y gestión de metadatos que en versiones anteriores de Kafka dependía de un servicio externo de coordinación. El clúster puede administrar topics, particiones, líderes y estado general sin necesitar un componente adicional separado para esa tarea.

**(b)** Agregar un servicio de coordinación externo al `docker-compose.yml` actual implicaría mezclar dos modelos de coordinación distintos, lo cual introduciría conflictos de configuración y se apartaría del diseño esperado del laboratorio, que ya está preparado para operar exclusivamente con KRaft y no con un esquema híbrido.

**(c)** La evidencia de que KRaft estaba funcionando se observó al poder crear el topic `pedidos-ventas`, listar topics, producir mensajes y consultar el estado del clúster desde Kafka UI, teniendo levantados únicamente los servicios de Kafka y Kafka UI. Es decir, operaciones como la creación del topic y la consulta de particiones se ejecutaron correctamente sin que existiera un segundo servicio de coordinación en ejecución.

---

## Pregunta 5 — Escalabilidad

Si el volumen de pedidos creciera 100× (de 1.000 a 100.000 mensajes por lote), ¿qué tres cambios se harían en: 

**(a)** El productor.

**(b)** El topic (particiones). 

**(c)** El consumer group. 

Justificar cada uno con conceptos de S6.

**Respuesta:**

**(a) Cambio en el productor.** Se ajustarían los parámetros de *batching* — `linger_ms`, `batch_size` y posiblemente la compresión — para que el productor agrupe mejor los mensajes antes de enviarlos y reduzca el overhead por mensaje. Con 100.000 mensajes por lote, enviar cada registro con demasiada frecuencia elevaría el costo de red y el tiempo total de publicación.

**(b) Cambio en el topic.** Se aumentaría el número de particiones. Con solo 4 particiones ya se observó desbalanceo en la ejecución actual, especialmente en `P0` (Bogotá con 390 mensajes y Cali con 155). Ante un crecimiento de 100×, ese *hot partition* se agravaría; más particiones permitirían distribuir mejor la carga y aumentar el paralelismo de consumo.

**(c) Cambio en el consumer group.** Se añadirían más consumidores activos para aprovechar ese mayor paralelismo. En este lab, un único consumidor procesó las 4 particiones. Ante un volumen de 100.000 mensajes, convendría escalar horizontalmente el consumer group para que varias instancias procesen en paralelo y reduzcan el tiempo total de consumo, respetando siempre que, dentro de un mismo grupo, cada partición solo puede asignarse a un consumidor a la vez.
