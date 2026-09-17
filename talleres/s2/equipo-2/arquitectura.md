---
title: Arquitectura

---

# Arquitectura — Equipo _2_ · Caso _Analítica retail_

> Copia este archivo a tu carpeta `equipo-N/arquitectura.md` y complétalo.
> No lo edites aquí en `plantillas/`.

## 1. Requisitos extraídos (los 4 ejes)

| Eje | Requisito del caso (con número, no lo inventes) |
|---|---|
| Latencia | El sistema debe refrescar los tableros de gerencia cada hora.|
| Throughput |El sistema debe procesar periódicamente la información proveniente de 400 tiendas físicas y del canal e-commerce para actualizar los tableros. |
| Consistencia |El sistema permite visualizar las actualizaciones y cambios de los tableros hasta 60min despues de hechos. |
| Costo / operación |El sistema usa una arquitectura tipo batch y permite el envió de información por paquetes en periodos de 1 hora, se contará con un único equipo de trabajo.  |

## 2. Ciclo de vida instanciado

| Etapa | Tecnología elegida | Justificación (cita al menos 1 eje) |
|---|---|---|
| Generación | Plataforma web, Sistemas de facturación físicos|Son las fuentes donde se originan las ventas y demás datos del negocio, generando la información que será recopilada y analizada posteriormente. |
| Ingesta |Batch Datos estructurados (Apache Airflow) |Permite orquestar procesos batch que se ejecutan de forma periódica, automatizando la ingesta de datos cada hora.|
| Almacenamiento |MinIO |Permite implementar un Data Lake para almacenar de forma segura y escalable el histórico de datos durante al menos 5 años, facilitando su consulta y reprocesamiento. |
| Transformación |Apache Spark | Permite procesar grandes volúmenes de datos mediante transformaciones distribuidas. Python y PySpark facilitan el desarrollo de reglas de negocio y cálculos analíticos sobre los datos almacenados.|
| Servicio | Power BI|Permite visualizar y analizar los datos procesados mediante dashboards interactivos, actualizados periódicamente para apoyar la toma de decisiones. |

## 3. Patrón de arquitectura elegido

- [ ] Lambda
- [ ] Kappa
- [x] Lakehouse (medallion)
- [ ] Híbrido — describir cuál combinación y por qué

**Justificación general del patrón** (3–5 líneas, citando ejes):

> Se eligió una arquitectura Lakehouse porque permite almacenar el histórico de cinco años en un Data Lake y, al mismo tiempo, procesar y consultar los datos de forma eficiente para generar los tableros gerenciales. Como el sistema solo requiere actualizaciones cada hora, una arquitectura batch reduce la complejidad operativa.

## 4. Diagrama

Pega tu diagrama en `diagrama.md` (bloque \`\`\`mermaid) o adjunta una
imagen exportada. Verifica que renderice antes del commit (usa
[mermaid.live](https://mermaid.live) para validar).

![image](https://hackmd.io/_uploads/BkrzaMNrMe.png)

## 5. Reflexión — la era agéntica

¿En qué etapa del ciclo de vida delegarían trabajo a un agente de IA, y
qué control humano mantendrían sobre esa etapa?

> Puede servir de apoyo en la etapa de servicio para ayudar en la presentación de los datos, determinar los puntos de intereses del usuario final, siempre y cuando haya consciencia de lo que realiza.

## 6. Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
|Creación de medidas DAX y columnas calculadas | Si| Es una actividad muy técnica en la que, con el uso de la IA, puede facilitar el trabajo|
|Jerarquía de gráficos | Si|Armonizar las visualizaciodes de los dashboards y el orden de presentación de gráficos según su importancia. |
|Asesoramiento de diseño | Si|Decidir paletas de colores, fuentes de letras que resulten más atractivos para un usuario final. |

> Recuerda: la extracción de requisitos, la elección del patrón y el
> ADR deben hacerse a mano (ver `docs/politica-ia.md`).
