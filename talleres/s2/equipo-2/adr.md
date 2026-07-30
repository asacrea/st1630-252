---
title: Adr

---

# ADR-001 — Arquitectura Lakehouse con procesamiento Batch

> Copia este archivo a tu carpeta `equipo-N/adr.md` y complétalo.
> Formato basado en Nygard, "Documenting Architecture Decisions" (2011).
>
> Elige la decisión MÁS DIFÍCIL o la que más debate generó en tu
> equipo — no la trivial. Un ADR sobre "usamos S3 para archivos" no
> aporta nada; un ADR sobre "elegimos Kappa aunque perdemos
> re-procesabilidad barata" sí.

## Estado

Propuesto / Aceptado / Reemplazado por ADR-XXX

## Contexto

¿Qué problema u obligación técnica estamos resolviendo? ¿Qué
restricciones del caso (requisitos, presupuesto, equipo) son
relevantes para esta decisión específica?

> El objetivo es centralizar la información de ventas de las 400 tiendas físicas y del canal e-commerce para generar reportes que se actualicen cada hora. Además, es necesario conservar un histórico de al menos cinco años y poder recalcular las métricas cuando cambien las reglas del negocio. Como el presupuesto es limitado y solo se dispone de un equipo de desarrollo, se requiere una solución que minimice la complejidad operativa sin sacrificar la capacidad de reprocesar la información histórica.

## Opciones consideradas

Como mínimo dos opciones reales (no una opción real vs. un "hombre de
paja" obviamente malo).

### Opción A: Arquitectura Lakehouse con procesamiento Batch

- Ventajas: 
1. Reduce la complejidad al manejar un único flujo de procesamiento.
2. Facilita el reprocesamiento del histórico cuando cambian las métricas.
3. Permite almacenar grandes volúmenes de datos a bajo costo mediante un Data Lake.
4. Es adecuada para actualizaciones periódicas (cada hora).
- Desventajas:
5. Requiere de mantenimiento constante por parte de los desarrolladores.

### Opción B: Arquitectura Lambda
- Ventajas:
1. Permite combinar procesamiento batch y streaming en una misma arquitectura.
2. Puede adaptarse fácilmente si en el futuro se requieren actualizaciones en tiempo real.
3. Mantiene un histórico mediante la capa batch.
- Desventajas:
1. Aumenta los costos de infraestructura y operación, lo que no se ajusta al presupuesto del caso.
2. Supone una mayor carga de mantenimiento para un único equipo de desarrollo.

## Decisión

¿Qué opción eligieron?

> Se escogió la opción A debido a que, además de que es un poco más económico que el Azure S3, también permite una fácil gestión de reprocesamiento histórico. Por otra parte, con esta alternativa, los datos solo le van a pertenecer a la empresa, no existe ninguna dependencia con terceros.

## Justificación

¿Por qué esta opción y no la otra? Cita explícitamente los ejes
(latencia, throughput, consistencia, costo) que inclinaron la balanza.

> Como la latencia no es el requisito dominante, una arquitectura lakehouse con procesamiento batch es suficiente para actualizar la información cada hora. Además, el enfoque Lakehouse permite almacenar grandes volúmenes de datos, conservar el histórico y reprocesarlo cuando sea necesario, utilizando herramientas de código abierto que reducen los costos y facilitan el mantenimiento por un único equipo de desarrollo.

## Consecuencias

¿Qué ganan con esta decisión? ¿Qué sacrifican o qué deuda técnica
asumen? ¿Qué tendría que cambiar en el futuro para revisitar esta
decisión?

> Con esta decisión se obtiene una arquitectura más sencilla de implementar y mantener, capaz de almacenar y procesar el histórico de datos de forma eficiente. También se reducen los costos al utilizar herramientas open source como Apache Airflow, MinIO y Apache Spark. No obstante, la información no estará disponible en tiempo real, si en el futuro el negocio requiere actualizaciones en tiempo real, será necesario reevaluar la arquitectura e incorporar tecnologías de streaming.