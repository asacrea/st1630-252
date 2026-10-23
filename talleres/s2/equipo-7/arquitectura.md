# Arquitectura — Equipo Andres Velez - Sebastian Salazar · Caso 1 (Fraude bancario)

## 1. Requisitos extraídos (los 4 ejes)

| Eje | Requisito del caso (con número) |
|---|---|
| Latencia | Decisión de bloqueo/aprobación en **< 300 ms** |
| Throughput | Volumen pico de **80.000 transacciones por segundo** |
| Consistencia | **Fuerte** — conciliación contable exacta a fin de día ("cuadran centavo a centavo" con el core bancario) + trazabilidad completa de cada decisión exigida por regulación estricta. No basta consistencia eventual porque un auditor debe poder reconstruir el estado exacto en cualquier momento, no solo al corte. |
| Costo / operación | **Alto** — sostener simultáneamente latencia < 300 ms, throughput de 80.000 tx/s y consistencia fuerte con trazabilidad exige infraestructura distribuida, redundante y auditable; no existe una opción "barata" que cumpla los tres ejes anteriores a la vez. |

## 2. Ciclo de vida instanciado

| Etapa | Tecnología elegida | Justificación (cita al menos 1 eje) |
|---|---|---|
| Generación | Sistemas transaccionales heterogéneos: tarjetas NFC, débito/crédito, app móvil, cajeros (ATM), puntos físicos (POS) | **Consistencia**: el monto debe registrarse exacto desde el origen, ya que la consistencia fuerte exigida (conciliación) depende de que el dato nazca correcto. |
| Ingesta | Kafka (log distribuido) para el speed layer, con recolección por lotes del mismo flujo para el batch layer | **Latencia**: el speed layer necesita cada transacción casi al instante para decidir en < 300 ms; el batch layer no requiere ingesta instantánea porque su cálculo ocurre a fin de día. |
| Almacenamiento | Redis (NoSQL en memoria) para el speed layer + S3 / object storage (lake) para el batch layer | **Latencia** (Redis permite lecturas/escrituras en microsegundos para decidir a tiempo) y **Costo** (object storage es barato para conservar el histórico completo del día). |
| Transformación | Spark Streaming (reglas de detección de fraude en tiempo real, sobre Kafka + Redis) + Spark Batch (recálculo exacto nocturno sobre el lake) | **Throughput** (80.000 tx/s sostenidas) y **Consistencia** (el batch layer recalcula la verdad exacta). Aquí se paga el costo operativo propio de Lambda: la lógica de negocio de detección de fraude vive duplicada entre el speed layer y el batch layer, y hay que mantenerlas sincronizadas. |
| Servicio | API de baja latencia que responde aprobar/bloquear al POS/ATM/app (respuesta síncrona) + reporte/dashboard interno de conciliación para el equipo de contabilidad (acceso restringido) | **Latencia** para la respuesta al cliente/canal; **Consistencia/confidencialidad** para el reporte de conciliación, que es información administrativa sensible. |

## 3. Patrón de arquitectura elegido

- [x] Lambda
- [ ] Kappa
- [ ] Lakehouse (medallion)
- [ ] Híbrido — describir cuál combinación y por qué

**Justificación general del patrón** (citando ejes):

> El caso tiene dos necesidades de latencia distintas conviviendo: bloqueo/aprobación
> instantáneo (< 300 ms) y conciliación contable que puede calcularse con calma
> a fin de día, pero debe ser exacta al centavo. Se evaluó Kappa (una sola base de
> código, releyendo el log completo para recalcular), pero el throughput pico de
> 80.000 tx/s implica reprocesar del orden de ~7.000 millones de eventos cada
> noche solo para la conciliación — un costo de cómputo recurrente alto. Se prefiere
> pagar el costo operativo de Lambda (mantener la lógica de fraude duplicada entre
> speed y batch layer) antes que ese costo de reprocesamiento diario completo,
> porque la parte batch no necesita ser rápida, solo exacta.

## 4. Diagrama

Ver `diagrama.md` (bloque Mermaid).

## 5. Reflexión — la era agéntica

> En este taller delegamos al agente la generación del código Mermaid del
> diagrama (ya diseñado en papel/conversación) y la resolución de dudas
> puntuales de tecnología (p. ej. qué es Redis, límites de throughput de
> Kafka). Mantuvimos control humano total sobre la extracción de los 4 ejes,
> la comparación Lambda vs. Kappa con números del caso, y la decisión final
> del patrón — que fue justamente la parte que generó más debate (throughput
> vs. costo de reprocesamiento) y donde el criterio de ingeniería no es
> sustituible por el agente.

## 6. Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Extracción de los 4 ejes (latencia, throughput, consistencia, costo) | No | Decisión central del taller; se hizo por diálogo guiado, respondiendo cada pregunta con criterio propio. |
| Comparación Lambda vs. Kappa y elección del patrón | No | Decisión de diseño central; el agente solo hizo de contraparte con preguntas, no propuso la respuesta. |
| Explicación de conceptos (Lambda, Kappa, Lakehouse, Redis, límites de Kafka) | Sí | Dudas puntuales de tecnología, expresamente permitido por la política del curso. |
| ADR completo (contexto, opciones, decisión, consecuencias) | No | Debe hacerse a mano según política del curso; se redactó a partir de la decisión ya tomada por el equipo. |
| Código Mermaid del diagrama | Sí | El diseño (5 etapas + speed/batch layer) ya estaba definido antes de pedir el Mermaid; el agente solo lo tradujo a sintaxis. |
| Redacción/formato final de las tablas del .md | Sí | Pulido de redacción sobre contenido ya decidido. |

