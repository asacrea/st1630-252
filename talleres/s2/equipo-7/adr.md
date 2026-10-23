# ADR-001 — Arquitectura Lambda para detección de fraude bancario en tiempo real

## Estado

Propuesto

## Contexto

El banco necesita bloquear o aprobar transacciones en menos de 300 ms,
con un volumen pico de 80.000 transacciones por segundo, y a la vez
producir una conciliación contable que cuadre centavo a centavo con el
core bancario al final del día, con trazabilidad completa exigida por
regulación estricta. Estas dos necesidades — reacción instantánea para
bloquear/aprobar, y exactitud total para conciliar — no tienen el mismo
requisito de latencia, lo que obliga a decidir entre mantener un solo
camino de procesamiento (Kappa) o dos caminos especializados (Lambda).

## Opciones consideradas

### Opción A: Kappa (todo como un único stream)

- Ventajas: una sola base de código para la lógica de detección de
  fraude, sin riesgo de que dos implementaciones diverjan; el log
  (Kafka) es la única fuente de verdad, lo que simplifica la trazabilidad.
- Desventajas: para producir la conciliación exacta de fin de día,
  el patrón implica releer el log del día completo. Con 80.000 tx/s
  sostenidas, eso equivale a reprocesar del orden de ~7.000 millones
  de eventos cada noche, un costo de cómputo recurrente alto para un
  cálculo que no necesita ser rápido, solo correcto.

### Opción B: Lambda (speed layer + batch layer)

- Ventajas: el speed layer responde en tiempo real sobre datos
  recientes (Redis) sin depender de reprocesar el histórico; el batch
  layer calcula la conciliación exacta sobre el lake (S3) sin presión
  de tiempo, leyendo el día ya consolidado en vez de recalcular desde
  el log crudo evento por evento.
- Desventajas: la lógica de negocio de detección de fraude debe
  mantenerse en dos implementaciones (streaming y batch), con el
  riesgo de que ambas se desalineen si se actualiza una regla en un
  lado y no en el otro.

## Decisión

Se elige **Lambda**: speed layer (Kafka + Spark Streaming + Redis)
para el bloqueo/aprobación en < 300 ms, y batch layer (S3 + Spark
Batch) para la conciliación exacta de fin de día.

## Justificación

La decisión se inclina por **throughput** y **costo/operación** sobre
la simplicidad de mantener una sola base de código: reprocesar todo el
log cada noche (Kappa) es más caro en cómputo recurrente que sostener
dos implementaciones de la lógica de fraude (Lambda), precisamente
porque el requisito de **latencia** de la conciliación es bajo (puede
calcularse "con calma" a fin de día) mientras que el de **consistencia**
es alto (debe ser exacta, no aproximada). Se acepta pagar el costo
operativo de duplicar la lógica de negocio a cambio de evitar el costo
de cómputo de reprocesar ~7.000 millones de eventos por noche.

## Consecuencias

Se gana un batch layer que calcula la conciliación sin comprometer el
rendimiento del speed layer, y un speed layer optimizado solo para
decidir rápido. Se sacrifica la simplicidad de una única base de
código: cada cambio en las reglas de detección de fraude debe
implementarse y probarse dos veces (streaming y batch), y debe
existir un proceso explícito para mantener ambas lógicas sincronizadas
(deuda técnica reconocida de Lambda). Si en el futuro el costo de
cómputo de releer el log completo bajara sustancialmente (por ejemplo,
con mejor particionamiento de Kafka o hardware más barato), valdría la
pena revisitar esta decisión y migrar hacia Kappa para eliminar la
duplicación de lógica.
