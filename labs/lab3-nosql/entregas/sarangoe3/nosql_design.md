# nosql_design.md — Lab 3: Comparativa de motores NoSQL

**Curso:** ST1630 Sistemas Intensivos en Datos · **Semana:** S10 · **Fecha:** 2026-10-26  
**Estudiante:** Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`)  
**Usuario del dataset:** `sarangoe3`  
**Huella de `verificar.py`:** `69ff0eda2cbe`  
**Resultado de `verificar.py`:** `21/21 verificaciones correctas · 0 con error · 0 pendientes`  

---

## Parte 1 — Documento y columnar ancho

### Pregunta 1 — El documento de MongoDB

#### 1. Datos del cliente y del producto embebidos vs. referenciados
- **Cliente:** Se embebieron los campos inmutables relevantes para la factura y consulta rápida: `cliente_id` y `nombre`. Se dejaron por fuera datos de contacto y perfil volátil como `email` y `segmento`. Si se requiere contactar al cliente o consultar su historial completo, se utiliza su clave foránea `cliente_id`.
- **Producto:** En el subdocumento `items` se copiaron `producto_id`, `nombre`, `cantidad` y `precio_unit`. 
- **Precio del catálogo vs. `precio_unit` pagado:** Se guardó estrictamente el **`precio_unit` pagado** en el momento de la transacción. Esto es crítico a nivel contable, legal y de negocio: un pedido representa un contrato cerrado en un instante de tiempo. Si el precio del catálogo cambia la próxima semana (por inflación, promociones o ajustes de catálogo), el pedido histórico jamás debe recalcularse con el nuevo precio, preservando la inmutabilidad de la transacción.

#### 2. Normalización vs. Desnormalización y cambio de nombre
Si un cliente cambia de nombre legalmente en su cuenta mañana, sus pedidos históricos mantendrán el nombre con el que se emitió cada pedido. En un sistema e-commerce esto **no es un error, sino el comportamiento correcto**: una factura o comprobante de compra debe reflejar los datos vigentes en la fecha de expedición. Como explica Martin Kleppmann (Cap. 2), el modelo de documento favorece la *localidad de almacenamiento* de agregados; forzar normalización relacional pura obligaría a hacer un `$lookup` (JOIN) costoso en cada lectura de pedido, rompiendo el objetivo de que **Q1** se resuelva en una sola operación de lectura $O(1)$ por `_id`.

---

### Pregunta 2 — La llave primaria de Cassandra

#### 1. DDL de las tablas creadas
```sql
CREATE TABLE IF NOT EXISTS ventas_por_region_dia (
    region text,
    dia date,
    fecha timestamp,
    pedido_id text,
    total int,
    PRIMARY KEY ((region, dia), fecha, pedido_id)
) WITH CLUSTERING ORDER BY (fecha DESC, pedido_id DESC);

CREATE TABLE IF NOT EXISTS pedidos_por_cliente (
    cliente_id text,
    fecha timestamp,
    pedido_id text,
    total int,
    PRIMARY KEY (cliente_id, fecha, pedido_id)
) WITH CLUSTERING ORDER BY (fecha DESC, pedido_id DESC);
```

#### 2. Justificación de Partition Key y Clustering Columns
- En `ventas_por_region_dia`, la **partition key** es la clave compuesta `(region, dia)`. Esto asegura que todos los pedidos de una misma región en una fecha específica queden agrupados en el mismo nodo y partición física.
- Las **clustering columns** son `(fecha DESC, pedido_id DESC)`. Dentro del SSTable en disco, Cassandra ordena físicamente las filas por timestamp descendente. Incluir `pedido_id` al final garantiza unicidad de celda en caso de que dos pedidos ocurran exactamente en el mismo microsegundo.
- En `pedidos_por_cliente`, la partition key es `cliente_id` y las clustering columns son `(fecha DESC, pedido_id DESC)`. Esto permite ejecutar `SELECT ... WHERE cliente_id = ? LIMIT 10` leyendo directamente la cabecera de la partición en memoria/disco en orden cronológico inverso sin tener que ordenar en memoria en Python.

#### 3. ¿Por qué no basta `region` como partition key?
Si se utilizara únicamente `region` como partition key:
- Con 500,000 pedidos diarios a lo largo de 1 año ($500{,}000 \times 365 = 182.500{,}000$ pedidos anuales), y considerando que Bogotá concentra el 40% del volumen:
  $$\text{Partición Bogotá} = 182.500{,}000 \times 0.40 = 73.000{,}000 \text{ filas}$$
- Una partición de 73 millones de filas violaría las recomendaciones operativas de Cassandra (donde una partición no debe superar los 100 MB ni los 100,000 registros). Generaría un grave problema de **Hot Partition**, desbalanceando la memoria JVM, saturando las pausas de Garbage Collection (GC) y degradando la compactación de SSTables. Con la clave compuesta `((region, dia))`, la partición más grande (Bogotá) tendrá como máximo:
  $$500{,}000 \times 0.40 = 200{,}000 \text{ filas diarias} \approx 20 \text{ MB}$$
  Una partición totalmente acotada, manejable y predecible en latencia.

---

### Pregunta 3 — Duplicar para consultar

En un motor relacional normalizado (3NF), duplicar un pedido en dos tablas sería considerado una redundancia peligrosa que expone al sistema a anomalías de actualización (*update anomalies*) y mayor consumo de almacenamiento. 

En Cassandra, **la duplicación por consulta es el patrón estándar de diseño**:
1. Cassandra está optimizada para escrituras de rendimiento masivo y secuencial (Append-only en CommitLog y MemTable con posterior *flush* a SSTables).
2. El disco es económico, pero la latencia de red y el costo de un JOIN distribuido entre nodos es prohibitivo.
3. Al modelar una tabla por cada consulta, cada patrón de lectura se atiende en $O(1)$ dirigiéndose a una sola partición de un solo nodo de réplica sin necesidad de escaneos globales ni `ALLOW FILTERING`.

**Si mañana el negocio pide "ventas por canal y día":**  
Se crea una nueva tabla optimizada específicamente para esa consulta:
```sql
CREATE TABLE IF NOT EXISTS ventas_por_canal_dia (
    canal text,
    dia date,
    fecha timestamp,
    pedido_id text,
    total int,
    PRIMARY KEY ((canal, dia), fecha, pedido_id)
) WITH CLUSTERING ORDER BY (fecha DESC, pedido_id DESC);
```
El productor o pipeline de ingesta escribirá en las 3 tablas de forma asíncrona o batch sin penalizar las consultas existentes.

---

## Parte 2 — Clave-valor y grafo

### Pregunta 4 — El top 10 en Redis

#### 1. Comparativa de tiempos de Q3
- **MongoDB (Aggregation Pipeline):** mediana **19.83 ms**
- **Redis (Sorted Set `ZREVRANGE`):** mediana **0.30 ms**
- **Factor de aceleración:** Redis fue **~66 veces más rápido**.

#### 2. ¿De dónde sale la diferencia?
- **Trabajo en escritura vs. lectura:** 
  - **MongoDB** guarda los documentos y en tiempo de lectura debe deserializar BSON, ejecutar `$unwind` sobre los arrays de ítems de 15,000 pedidos (más de 28,000 ítems), acumular sumas en un hash map en memoria RAM y ejecutar un sort sobre los 1,200 productos. Todo el trabajo analítico se realiza en el momento de la consulta.
  - **Redis** traslada ese costo al momento de la escritura. Cada vez que entra una venta, ejecuta `ZINCRBY` en $O(\log N)$ actualizando una estructura de datos híbrida de *SkipList* + *HashTable* en memoria RAM. Cuando se consulta Q3, el ranking ya está completamente precalculado y ordenado; responder la consulta solo requiere leer los primeros 10 punteros de la lista en memoria ($O(\log N + M)$).

#### 3. Persistencia y uso como base de datos principal
- Si el contenedor de Redis se reinicia sin persistencia (RDB snapshot o AOF fsync=always), todos los datos en memoria RAM se pierden por completo.
- **¿Usarías Redis como BD principal de pedidos?** **No.** Redis no está diseñada para ser el almacenamiento durable del historial de pedidos:
  1. El costo de RAM por gigabyte es órdenes de magnitud superior al almacenamiento NVMe/SSD de MongoDB o Cassandra.
  2. No admite consultas ad-hoc complejas no modeladas de antemano (filtros multidimensionales por fechas, rangos, clientes).
  3. No garantiza transacciones ACID multi-documento con durabilidad estricta sin sacrificar su ventaja de latencia. Su rol ideal es como capa de caché, ranking en vivo y leaderboard en tiempo real junto a un almacén durable.

---

### Pregunta 5 — Recomendaciones: documento vs. grafo

#### 1. Pasos y tiempos de Q4
- **MongoDB:**
  - *Pasos:* 3 etapas en Python/Mongo (1. Búsqueda de productos propios con `distinct`; 2. Búsqueda de clientes vecinos con `$in` y `$ne`; 3. Búsqueda de pedidos de vecinos y agregación de intersecciones en memoria Python).
  - *Medianas de tiempo:* C00146: **34.16 ms**, C00438: **21.40 ms**, C00363: **49.65 ms** (Promedio: ~**35 ms**).
- **Neo4j:**
  - *Pasos:* 1 sola consulta declarativa en Cypher utilizando traversals directos en el grafo.
  - *Medianas de tiempo:* C00146: **95.88 ms**, C00438: **70.23 ms**, C00363: **140.21 ms** (Promedio: ~**102 ms**).

#### 2. ¿Por qué MongoDB fue más rápido en esta medición y cuándo gana Neo4j?
En este dataset específico (2,700 nodos y 28,155 relaciones), el volumen cabe holgadamente en el buffer pool de MongoDB, y el script de Python resolvió el conteo de frecuencias en memoria local tras traer los datos con índices multikey B-Tree. Por su parte, la llamada a Neo4j incluye el overhead de serialización a través del protocolo Bolt sobre TCP, la compilación del plan de ejecución Cypher y el cálculo de `count(DISTINCT vecino)`.

**¿Cuándo gana Neo4j?**  
Neo4j gana rotundamente cuando:
1. El grafo escala a millones de nodos y cientos de millones de relaciones, donde MongoDB requeriría traer gigabytes de documentos por la red hacia la aplicación para calcular intersecciones de conjuntos en Python.
2. Neo4j implementa **Index-Free Adjacency**: cada nodo almacena punteros de memoria directos a sus relaciones y nodos vecinos; recorrer una arista es una desreferenciación de puntero en memoria en tiempo $O(1)$, sin búsquedas en índices B-Tree.

#### 3. Si la consulta cambiara a tres saltos:
- En **MongoDB**, una recomendación de 3 o 4 saltos ("amigos de amigos de productos...") requiere una explosión cartesiana de `$lookup` anidados o múltiples rondas de queries iterativas entre la base de datos y la aplicación, volviéndose inmanejable y generando latencias de varios segundos.
- En **Neo4j**, la consulta solo cambia en la sintaxis de longitud de camino variable (e.g. `[:COMPRO*1..3]`), manteniendo una navegación directa por punteros de memoria sin JOINs relacionales ni consultas múltiples.

---

## Parte 3 — Stream con reentregas

### Pregunta 6 — Cuánto se equivoca el sink ingenuo

#### 1. Números empíricos de la corrida
- Entregas procesadas por el stream: **6,200**
- Eventos únicos reales: **5,000**
- Unidades reales vendidas: **7,005**
- Redis ingenuo (`ZINCRBY` incondicional): **8,704** unidades
- **Error del sink ingenuo:** **+1,699 unidades de más** (sobreconteo de +24.25%)
- Sinks idempotentes implementados (Redis y Cassandra): **7,005 unidades** (exactitud del 100%).

#### 2. Las dos fuentes de duplicados
1. **Reentregas inherentes al transporte *at-least-once* (20% del dataset):** De los 5,000 eventos generados, aproximadamente 1,000 eventos ya venían duplicados deliberadamente en el stream (simulando reintentos del productor en red o Kafka rebalances).
2. **Caída del consumidor en el evento 3,700:** El consumidor confirmaba su progreso cada `COMMIT_CADA = 500` eventos. El último commit exitoso ocurrió en el evento 3,500. Al caerse en el evento 3,700, el consumidor reinició desde el último offset guardado (3,500), provocando la reentrega de los **200 eventos** comprendidos entre 3,500 y 3,700.

---

### Pregunta 7 — Idempotencia en Redis y en Cassandra

#### 1. Mecanismo de idempotencia en Redis
- Se implementó deduplicación atómica mediante el comando `SET key value NX EX seconds`:
  ```python
  es_nuevo = r.set(f"visto:{evento['event_id']}", "1", nx=True, ex=3600)
  if es_nuevo:
      r.zincrby(CLAVE_RANKING, evento["cantidad"], evento["producto_id"])
  ```
- **Control de memoria:** Se utilizó un tiempo de vida (TTL) de 1 hora (`ex=3600`). En producción, la ventana de reentregas de Kafka o Kinesis suele estar en el orden de segundos o minutos; fijar un TTL permite que las llaves expiren automáticamente sin consumir memoria de manera infinita.
- **Concurrencia:** Dado que Redis es de ejecución monohilo (single-threaded event loop), la operación `SET ... NX` es **estrictamente atómica**. Si dos consumidores procesan el mismo `event_id` simultáneamente, solo uno recibirá `True` y ejecutará el incremento; el segundo recibirá `None` y descartará el evento, garantizando idempotencia concurrente sin condiciones de carrera.

#### 2. Idempotencia en Cassandra
- En la tabla `ventas_stream`, la clave primaria es:
  `PRIMARY KEY (producto_id, event_id)`
- En Cassandra, una sentencia `INSERT` con una clave primaria preexistente no genera error ni crea un registro duplicado: realiza una **sobreescritura idempotente (Upsert)** del registro en el MemTable con el timestamp más reciente. Si el mismo `event_id` se recibe 10 veces, la fila se sobreescribe 10 veces con los mismos valores exactos, dejando el estado final de la partición inalterado.
- **¿Por qué las columnas `counter` de Cassandra NO serían idempotentes?**  
  Las columnas de tipo `counter` en Cassandra se actualizan mediante `UPDATE ... SET x = x + c`. No son idempotentes porque ante un reintento de red, fallo temporal o reentrega del stream, Cassandra volverá a ejecutar el incremento aritmético `+ c`, sumando dos o más veces la misma venta en la tabla.

---

## Parte 4 — Comparativa

### Pregunta 8 — Tabla comparativa con números reales

| Dimensión | MongoDB | Cassandra | Redis | Neo4j |
|---|---|---|---|---|
| **Modelo de datos** | Documento (BSON jerárquico) | Columnar ancho (Wide-column family) | Clave-Valor en memoria (Data structures) | Grafo (Nodos, Etiquetas, Relaciones tipadas) |
| **Tiempo de carga (s)** | **0.3 s** (15,000 docs) | **10.8 s** (30,000 filas en 2 tablas) | **0.3 s** (1,200 hashes + zset) | **8.2 s** (2,700 nodos, 28,155 relaciones) |
| **Consultas resueltas y mediana (ms)** | Q1: **0.40 ms**<br>Q3: **19.83 ms**<br>Q4: **34.16 ms** | Q2a: **5.05 ms**<br>Q2b: **4.57 ms** | Q3: **0.30 ms** | Q4: **95.88 ms** |
| **Qué consulta le resultó natural** | **Q1:** Lectura de pedido completo en 1 solo seek de `_id`. | **Q2a y Q2b:** Lectura de partición única ordenada por disco con clustering keys. | **Q3:** Ranking en tiempo real precalculado en Sorted Set. | **Q4:** Recomendaciones de compras compartidas navegando aristas de grafo. |
| **Qué consulta le resultaría difícil o imposible** | Relaciones N:M profundas (más de 3 saltos de grafos). | Consultas ad-hoc no modeladas en la clave primaria (requiere `ALLOW FILTERING`). | Búsquedas ad-hoc por atributos internos sin clave directa. | Agregaciones analíticas masivas sobre millones de transacciones de un día. |
| **Cómo escala (más datos, más nodos)** | *Sharding* por clave de partición; balanceo automático de chunks. | *Consistent Hashing* maestro-maestro (Peer-to-Peer) con particionado lineal infinito. | *Redis Cluster* (16,384 hash slots) particionando claves en RAM. | Escalado vertical para escrituras; réplicas de solo lectura para consultas de grafos. |

---

### Pregunta 9 — Si solo pudieras elegir uno

#### 1. Selección para el negocio
Si este e-commerce solo pudiera operar un motor para todo su núcleo transaccional de pedidos, el motor elegido sería **MongoDB**:
- Es el único de los cuatro que permite almacenar la entidad principal (**Pedido**) como un agregado completo y coherente con lectura $O(1)$ en **Q1**.
- Ofrece la mayor versatilidad: soporta índices secundarios, índices multikey para arrays, agregaciones complejas (como **Q3** en 19 ms) y consultas ad-hoc que el negocio inevitablemente pedirá a futuro.
- **Qué se sacrificaría:** Se sacrificarían las latencias sub-milisegundo de Redis (0.3 ms) para el ranking en tiempo real y la elegancia nativa de Cypher para grafos profundos de recomendación.

#### 2. Cassandra: Factor de replicación 1 vs. Factor 3 con QUORUM
- **Factor de Replicación 1:** No tiene tolerancia a fallos. Si el nodo único se cae o su disco falla, el 100% de los datos que almacena quedan inaccesibles de inmediato (indisponibilidad total) y no hay réplicas de respaldo.
- **Factor de Replicación 3 con consistencia `QUORUM`:**
  - *Ganancia:* Alta disponibilidad y tolerancia a particiones de red. El sistema puede soportar la caída completa de 1 nodo sin perder disponibilidad ni consistencia de lectura/escritura, ya que:
    $$Q = \left\lfloor \frac{R}{2} \right\rfloor + 1 = \left\lfloor \frac{3}{2} \right\rfloor + 1 = 2 \text{ nodos requeridos}$$
    Dado que $W + R > N$ ($2 + 2 = 4 > 3$), se garantiza consistencia estricta (*Strong Consistency*) y se leen siempre los datos más recientes.
  - *Costo pagado:* Mayor latencia de escritura y lectura (cada operación debe esperar la confirmación de al menos 2 nodos sobre la red en lugar de responder localmente), mayor tráfico de red inter-nodo y el triple de costo de almacenamiento en disco.
