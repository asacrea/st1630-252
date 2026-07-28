# Arquitectura — Equipo __ · Caso __

> Copia este archivo a tu carpeta `equipo-N/arquitectura.md` y complétalo.
> No lo edites aquí en `plantillas/`.

## 1. Requisitos extraídos (los 4 ejes)

| Eje | Requisito del caso (con número, no lo inventes) |
|---|---|
| Latencia | los tableros se refrescan cada hora|
| Throughput | la información viene de una cadena de 400 tiendas + canal e-commerce |
| Consistencia | consistencia eventual. Necesita histórico de 5 años |
| Costo / operación |Presupuesto ajustado: no pueden mantener dos equipos de ingeniería (uno para batch, otro para streaming). |

## 2. Ciclo de vida instanciado

| Etapa | Tecnología elegida | Justificación (cita al menos 1 eje) |
|---|---|---|
| Generación | 400 tiendas y e-commerce |La generación de datos se da através de las fuentes ya mencionadas haciendo referencia a el eje de Throughput. |
| Ingesta |Apache Airflow | Permite programar y orquestar cargas periódicas, batch, una ejecución cada hora satisface al latencia requerida. |
| Almacenamiento | Amazon S3 | Ofrece almacenamiento económico escalable con tablas en formato deltalake que permite el versionamiento transacciones ACID y prepocesamiento historico de 5 años cumpliendo con el eje de consistencia y costo (porque es económico).  |
| Transformación | Apache Spark | Permite procesar el volumen generado por las 400 tiendas, particionando por capas ya que facilita calcular las métricas cuando cambie una definición hace referencia a el eje de Throughput. |
| Servicio | Datawarehouse SQL y Power BI | Power BI puede actualizar los tableros cada hora sin necesidad de consultas en tiempo real sobre las fuentes transaccionales. haciendo referencia a el eje de consistencia y al de latencia.|

## 3. Patrón de arquitectura elegido

- [ ] Lambda
- [ ] Kappa
- [x] Lakehouse (medallion)
- [ ] Híbrido — describir cuál combinación y por qué

**Justificación general del patrón** (3–5 líneas, citando ejes):

> Teniendo en cuenta lo que se vio en clase, el Lakehouse es la mejor opción porque primero según el caso tiene  baja latencia (no se requiere tener los datos actualizandose en tiempo real),sino que se busca una gran facilidad para almacenar grandes volumenes de datos (400 tiendas y e-commerce) y re-procesarla, ademas evita mantener flujos separados lo que reduce costos y complejidad operativa.  

## 4. Diagrama

Pega tu diagrama en `diagrama.md` (bloque \`\`\`mermaid) o adjunta una
imagen exportada. Verifica que renderice antes del commit (usa
[mermaid.live](https://mermaid.live) para validar).

## 5. Reflexión — la era agéntica

¿En qué etapa del ciclo de vida delegarían trabajo a un agente de IA, y
qué control humano mantendrían sobre esa etapa?

> Lo delegariamos a servicio porque, nos ayuda en la parte de entregar valor, ya sea por medio de analítica de datos para luego transformarlas a un formato facil de entender (Power BI) ya que consideramos que la IA modela muy bien las estadísticas o métricas de datos.   

## 6. Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Tecnologias en el ciclo de vida |  ChatGPT | Dado a nuestro bajo conocimiento en tecnologias que se puedan implementar en el almacenamiento, ingesta, transformación y servicio se uso la IA para que nos recomiende cuales tecnologias son las más adecuadas en estos casos.|
| | | |

> Recuerda: la extracción de requisitos, la elección del patrón y el
> ADR deben hacerse a mano (ver `docs/politica-ia.md`).
