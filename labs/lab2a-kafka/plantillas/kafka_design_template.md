# Diseño Kafka — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Copia este archivo a tu carpeta de entrega como `kafka_design.md` y
> complétalo después de tener el productor y el consumidor corriendo.
> Cada respuesta debe citar evidencia concreta de TU propia ejecución
> (números de Kafka UI, tus propios logs) — una respuesta que solo
> repite la teoría de clase sin conectarla con tu pipeline no cuenta
> como completa (ver la Rúbrica en `../README.md`).

## Pregunta 1 — Garantía elegida

Elegiste at-least-once para este lab. Justifica en términos del modelo
de commit de offset y de la idempotencia del MERGE Delta: ¿qué pasa
exactamente si el consumidor falla después del MERGE pero antes del
commit? ¿Cuántas veces procesará Kafka ese mensaje? ¿Por qué el
resultado en Bronze es el mismo?

> Pista: esto es literalmente lo que probaste en la Parte 2.4 del lab
> (prueba de idempotencia) — cita tus propios números.

→ [tu respuesta aquí]

## Pregunta 2 — Decisión de key

Elegiste `key=region` como clave del productor. Responde:

(a) ¿Qué garantía de orden provee?
(b) ¿Qué problema de balanceo genera, dado que Bogotá tiene ~40% del
    tráfico y el topic tiene 4 particiones?
(c) ¿Qué clave alternativa usarías si el orden no importara pero el
    balanceo fuera crítico? Justifica.

> Pista: tu script imprime un resumen región → partición → mensajes al
> final — úsalo como evidencia para (b), no una cifra inventada.

→ [tu respuesta aquí]

## Pregunta 3 — Número de particiones

El topic tiene 4 particiones y el consumer group tiene 1 consumidor.
¿Cuántas particiones lee ese consumidor? ¿Cuál es el máximo de
consumidores activos que puedes añadir sin que ninguno quede ocioso?
¿Qué pasaría si añadieras 6?

→ [tu respuesta aquí]

## Pregunta 4 — KRaft

El `docker-compose.yml` de este lab usa KRaft. Responde:

(a) ¿Qué hace KRaft que antes hacía el modelo de coordinación externa
    (previo a Kafka 4.0)?
(b) ¿Qué crees que pasaría si intentaras agregar un servicio de
    coordinación externa adicional al `docker-compose.yml` existente?
(c) ¿En qué momento del lab viste evidencia de que KRaft estaba
    funcionando? (pista: cualquier comando que le pregunte algo al
    clúster sin que exista un segundo servicio de coordinación corriendo)

→ [tu respuesta aquí]

## Pregunta 5 — Escalabilidad

Si el volumen de pedidos creciera 100× (de 1.000 a 100.000 mensajes
por lote), ¿qué tres cambios harías en este lab? Justifica cada uno
citando conceptos de S6:

(a) Un cambio en el productor
(b) Un cambio en el topic (particiones)
(c) Un cambio en el consumer group

→ [tu respuesta aquí]
