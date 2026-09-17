# ADR-001: Decisión operativa aproximada en tiempo real vs. verdad contable exacta en batch

**Equipo:** Hellen Yanes Doria y Samuel Samper Cardona — Caso 1 (Fraude bancario)
**Fecha:** 2026-07-23
**Estado:** Aceptada

## Contexto

El caso exige dos cosas que, en el mismo pipeline, son mutuamente excluyentes:

1. Una decisión de **aprobar/bloquear** la transacción en **menos de 300 ms**, sobre un pico de **80.000 tx/s**.
2. Una **conciliación contable exacta** a fin de día, en un entorno con **regulación estricta** que exige que el ledger final sea auditable y correcto.

El sistema de scoring en tiempo real solo puede ver una ventana reciente de estado (features del feature store, últimas N transacciones de la cuenta). No puede, en 300 ms, consultar la historia completa ni esperar a que un job de reconciliación confirme el saldo real de la cuenta. Es decir: **la decisión operativa en caliente es necesariamente aproximada**, mientras que el registro contable no puede serlo.

La pregunta que dividió al equipo fue: ¿construimos un único pipeline que sea "lo bastante exacto" para ambos propósitos, o aceptamos que son dos pipelines con dos niveles de verdad distintos y diseñamos el proceso para reconciliarlos?

## Opciones consideradas

**Opción A — Kappa pura (un solo pipeline, reprocesar el log para exactitud).**
Todo — decisión en caliente y conciliación — se resuelve reprocesando el mismo stream. Evita mantener dos rutas de código.
*Descartada porque:* reprocesar 80.000 tx/s históricas cada vez que se necesita el número exacto del día es costoso y no resuelve el límite físico de 300 ms para la decisión operativa: seguiría siendo necesario un resultado aproximado en caliente en algún punto.

**Opción B — Lambda clásica (batch layer + speed layer con lógica de negocio duplicada).**
Un pipeline batch que calcula "la verdad" y un pipeline streaming que calcula "lo aproximado", cada uno con su propia implementación del scoring/agregación.
*Descartada porque:* en un entorno con regulación estricta, mantener dos implementaciones de la misma lógica de negocio (dos lugares donde definir "qué es una transacción válida") es exactamente el tipo de deuda que un auditor penaliza — el riesgo de que las dos lógicas diverjan sin que nadie lo note es alto.

**Opción C — Híbrida: un solo log fuente de verdad (Kafka), leído por una capa streaming operativa (aprobar/bloquear, aproximada, reversible) y una capa batch de lakehouse (ledger, exacta, autoritativa) — elegida.**
La decisión en caliente es tratada explícitamente como *provisional y reversible* (bloqueo, retención, contracargo), no como el número final. El ledger del batch nocturno es la única fuente de verdad contable.

## Decisión

Adoptamos la **Opción C**. La capa en tiempo real no compite por ser "la verdad": su contrato es proteger al usuario y al banco en el momento (falso positivo se revisa, falso negativo se corrige por contracargo), mientras que el cierre contable de fin de día sobre el lakehouse es el único número que se reporta al regulador.

Esto obliga a diseñar explícitamente un **proceso de reconciliación y reversión**: cuando el batch nocturno detecta una discrepancia respecto a lo que decidió el sistema en caliente (por ejemplo, una transacción aprobada que el análisis exacto marca como fraude), el sistema debe generar automáticamente un caso de contracargo/reverso, no simplemente "corregir un número" silenciosamente.

## Consecuencias

**Positivas**
- Cumple simultáneamente los 4 ejes: latencia y throughput en la capa operativa, consistencia y auditabilidad en la capa batch.
- Una sola fuente de verdad de eventos (Kafka) reduce el riesgo de lógica de negocio duplicada frente a Lambda clásica.
- El sistema es honesto sobre su propia incertidumbre: nadie asume que la decisión en 300 ms es definitiva.

**Negativas**
- Se necesita construir y operar un proceso de reconciliación/reversión que no existiría en un diseño más simple — es complejidad adicional, justificada solo porque el eje de consistencia regulatoria lo exige.
- Requiere gobernanza explícita del modelo de fraude (falsos positivos molestan al cliente, falsos negativos generan pérdida y riesgo regulatorio) — no es un problema puramente técnico, es un problema de política que el equipo de negocio debe definir junto con ingeniería.
- Dos rutas de lectura del mismo log (streaming y batch) siguen siendo dos piezas de infraestructura que operar y monitorear, aunque compartan la misma lógica fuente.
