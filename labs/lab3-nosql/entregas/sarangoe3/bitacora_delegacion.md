# bitacora_delegacion.md — Lab 3

**Estudiante:** Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`)

> Conforme a la política institucional en `docs/politica-ia.md`, se detalla a continuación el alcance de uso y delegación para cada tarea del laboratorio.

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Qué embeber en el documento de MongoDB (TODO 1) | **No** | Decisión de diseño propia: decidí embeber `cliente_id` y `nombre` del cliente, y en los ítems el `precio_unit` pagado históricamente (no el del catálogo) para que Q1 sea resuelto en una sola lectura $O(1)$ sin JOINs. |
| Índices de MongoDB (TODO 2) | **Parcial** | Diseñé los índices necesarios para las consultas (multikey en `items.producto_id` y en `cliente.cliente_id`); el agente ayudó a verificar la sintaxis de `create_index`. |
| Q3 y Q4 en MongoDB (aggregation pipelines) | **Parcial** | Diseñé la lógica de `$unwind` + `$group` para Q3 y la lógica de vecinos para Q4; el agente ayudó a depurar la sintaxis del aggregation pipeline. |
| `PRIMARY KEY` de `ventas_por_region_dia` y `pedidos_por_cliente` | **No** | Decisión de arquitectura propia: elegí la partition key compuesta `((region, dia))` para evitar hot partitions de 73M de filas en Bogotá, y clustering `(fecha DESC, pedido_id DESC)` para ordenar en disco. |
| Consultas Q2a y Q2b en CQL | **No** | Diseñé las consultas para leer una sola partición sin `ALLOW FILTERING` y obtener los 10 o 5 pedidos más recientes directamente del orden de almacenamiento. |
| Estructuras y consulta de Redis | **Parcial** | Decidí usar Hashes para el catálogo y Sorted Sets para el ranking de unidades; el agente ayudó con la sintaxis de pipelines y la resolución del desempate ascendente con `zrevrangebyscore`. |
| Cypher de carga y de Q4 en Neo4j | **Parcial** | Modelé el grafo con relaciones `(:Cliente)-[:COMPRO]->(:Producto)` y diseñé la consulta de recomendaciones de 2 saltos; el agente asistió con la sintaxis Cypher de `UNWIND` y `MERGE`. |
| Mecanismo de idempotencia en Redis (Parte 3) | **No** | Decidí utilizar `SET seen:event_id 1 NX EX 3600` como mecanismo atómico de deduplicación con TTL para acotar el uso de memoria RAM. |
| Tabla `ventas_stream` y su llave primaria (Parte 3) | **No** | Diseñé la clave primaria `(producto_id, event_id)` para que las reentregas de eventos se absorban como sobreescrituras naturales idempotentes (Upserts) en Cassandra. |
| Diagrama de los modelos en draw.io | **Parcial** | Diseñé la estructura conceptual de representación del mismo pedido en los 4 motores; el agente ayudó a renderizar y exportar el archivo XML `.drawio` y PNG. |
| Redacción de `nosql_design.md` | **Parcial** | Proporcioné los números reales de mi corrida, mis justificaciones de diseño y comparativas; el agente ayudó a estructurar el formato Markdown. |
| Troubleshooting de Docker y drivers | **Sí** | Se delegó la configuración de contenedores Docker, levantamiento de servicios y ajuste de puertos. |
