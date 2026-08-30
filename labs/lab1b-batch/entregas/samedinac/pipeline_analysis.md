# Análisis del pipeline — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 23/08/2026
**Estudiante:** Sebastian Andres Medina Cabezas - samedinac@eafit.edu.co

## Pregunta 1 — Exchange del pipeline completo

¿Cuántos `Exchange` tiene el pipeline completo (Bronze → Silver →
Gold)? Identifica a qué operación corresponde cada uno y explica en
términos físicos (shuffle write, shuffle read) por qué esa operación es
WIDE.

→ Total de Exchanges en el Pipeline: 7
1. Capa Bronze (0 Exchanges)
Toda la ingesta a Bronze es una operación NARROW. Las transformaciones que hicimos (withColumn para la fecha y el origen) se aplican fila por fila de manera local en cada partición. Ningún executor necesitó hablar con otro para escribir el dato crudo en modo append.

2. Capa Silver (2 Exchanges)

dropDuplicates(): Para saber si una fila está duplicada, Spark no puede mirar solo una partición. Físicamente, hace un Shuffle Write: cada executor toma sus filas, calcula un hash basado en las columnas, y agrupa los hashes en buckets locales. Luego ocurre el Shuffle Read: los executors se envían por red esos buckets para que todas las filas idénticas caigan en el mismo nodo, se comparen y se descarte la copia.

MERGE (Escritura Delta): Para saber si un pedido_id ya existe y actualizarlo (o insertarlo si es nuevo), Spark tiene que hacer un JOIN por debajo entre tu DataFrame y la tabla que ya existe en S3. Esto requiere alinear las particiones por el hash de pedido_id, forzando otro intercambio de datos por red.

3. Capa Gold (5 Exchanges)

KPI 1 - groupBy("region", "fecha"): Shuffle para mandar todos los pedidos de la misma región y fecha al mismo executor y poder sumarlos.

KPI 2 (Paso 1) - groupBy("categoria", "producto"): Shuffle para agrupar y sumar el total de cada producto.

KPI 2 (Paso 2) - Window.partitionBy("categoria"): Un shuffle distinto al anterior. Spark tiene que reagrupar (Shuffle Write/Read) para que todos los productos de una misma categoría queden juntos en memoria y poder aplicar el rank().

KPI 3 - groupBy("canal", "metodo_pago"): Shuffle para construir las cohortes que diseñaste.

OPTIMIZE ... ZORDER BY: Para reordenar físicamente la tabla y compactarla, Spark tiene que leer todos los archivos Parquet pequeños, barajar (shuffle) todos los datos a través de la red basándose en fecha y region, y escribir archivos grandes y organizados.

Una operación genera un Exchange (es WIDE) cuando requiere un Shuffle Write (los ejecutores calculan particiones destino y escriben en su disco local) seguido de un Shuffle Read (los ejecutores solicitan y descargan por red los datos de los demás). Esto es obligatorio siempre que necesitemos agrupar (groupBy), ordenar globalmente (Window/Zorder), o cruzar datos (Merge/dropDuplicates) basándonos en una clave común, ya que los datos de esa clave originalmente están esparcidos al azar por todo el clúster."

## Pregunta 2 — Recalcular vs. filtrar `total`

Elegiste recalcular `total` desde `cantidad × precio_unit` en vez de
filtrar las filas con `total` incorrecto. ¿Cuántas filas preservaste
con esta decisión vs. filtrar directamente por `total` inválido?
¿Cuándo NO sería correcto recalcular?

→ 
1. Filas preservadas:

Al tomar la decisión de recalcular el total (multiplicando cantidad × precio_unit), logramos preservar 3,959 filas (2,571 con nulos, 926 con negativos y 462 con ceros). Si hubiéramos optado por un filtrado estricto (eliminar los registros inválidos), habríamos descartado el 3.90% del dataset original. Recalcular nos permitió salvar información valiosa sobre el comportamiento de esos clientes, los productos que compraron y las fechas de transacción, limpiando el error de sistema sin perder el evento de negocio.

2. ¿Cuándo NO sería correcto recalcular?

No sería correcto recalcular el total con una simple multiplicación si el sistema de origen incluye lógicas de negocio ocultas que no tenemos en las columnas base. Por ejemplo:

Descuentos o cupones: Si el cliente aplicó un código promocional en el carrito, el total real pagado será menor que cantidad × precio_unit.

Impuestos y envíos: Si el total crudo incluye el IVA o el costo de envío.

## Pregunta 3 — Robustez de la normalización de región

Para la normalización de región usaste `upper(trim())` + `when()` para
aliases. ¿Qué pasaría con una variante nueva que llegue la próxima
semana (`'BOG'`, `'Bgo'`)? ¿Cómo harías el pipeline más robusto sin
tener que reescribirlo cada vez que aparece una variante nueva?

→ 
1. ¿Qué pasaría con una variante nueva?

Con el enfoque actual basado en when().otherwise(), la lógica está 'quemada' en el código. Si la próxima semana ingresan las variantes 'BOG' o 'Bgo', el motor evaluará upper(trim()) (quedando como 'BOG' y 'BGO'), pero al no encontrar coincidencias exactas en la cadena de condiciones, el registro caerá en la cláusula .otherwise(). Esto significa que se clasificarán incorrectamente como 'OTRO' (o el valor por defecto asignado), afectando la calidad de los reportes en la capa Gold sin que el sistema arroje ningún error."

2. ¿Cómo hacer el pipeline más robusto?

Para evitar reescribir y redesplegar el código fuente del pipeline ante cada nueva variante, se debe desacoplar la lógica de mapeo del código. La mejor práctica es implementar una Tabla de Homologación (Lookup / Dimension Table):

Diseño: Crear una tabla o archivo pequeño en S3 (ej. s3://.../config/region_mapping.csv) con dos columnas: variante_cruda y region_canonica.

Implementación en Spark: En lugar de hacer una cadena de when(), el script Silver lee esta tabla de configuración y realiza un Broadcast Hash Join (un LEFT JOIN optimizado para tablas pequeñas) cruzando el texto original del pedido contra la tabla de homologación.

Mantenimiento Operativo: Si aparece un nuevo alias como 'BOG', un analista de datos o Data Steward simplemente agrega una nueva fila al archivo maestro de homologación (BOG -> Bogotá). En la siguiente ejecución, el pipeline de Spark leerá la regla actualizada y la aplicará automáticamente, logrando un proceso guiado por metadatos donde no se requiere intervención de un ingeniero para modificar el script.

## Pregunta 4 — Partición y shuffle files

Ajustaste `spark.sql.shuffle.partitions=32`. Con 101.500 filas y 32
particiones: ¿cuántas filas por partición, en promedio? ¿Qué pasaría
con el valor por defecto de 200 particiones? Calcula el número de
shuffle files que genera el MERGE con 200 particiones vs. 32, en un
clúster de 4 executors.

→ 
1. Filas por partición (con 32 particiones): Al configurar spark.sql.shuffle.partitions=32, las 101.500 filas se distribuyen entre 32 particiones. En promedio, cada partición procesará 3.171 filas (101.500 / 32). Este es un tamaño mucho más razonable para el volumen de datos de este laboratorio, permitiendo que cada uno de los 32 cores de nuestro clúster (4 executors × 8 cores) procese un bloque de datos decente sin quedarse inactivo.
2. ¿Qué pasaría con el valor por defecto de 200? Si hubiéramos dejado el valor por defecto de 200 particiones, tendríamos apenas 507 filas por partición (101.500 / 200). Esto genera un problema de sobre-particionamiento (over-partitioning): Spark perdería más tiempo planificando, coordinando tareas (Task Scheduling) y manejando los metadatos de red que procesando los datos reales. Las tareas terminarían en milisegundos, pero el 'overhead' del motor haría el proceso global más lento.
3. Cálculo de Shuffle Files (MERGE):"Físicamente, la cantidad de bloques o archivos generados durante un Shuffle se calcula multiplicando el número de tareas de mapeo ($M$) por el número de particiones de reducción ($R$, definido por spark.sql.shuffle.partitions). Asumiendo que el paso de lectura (Map) aprovecha los 32 cores del clúster ($M=32$):Con 200 particiones: $32 \text{ (Maps)} \times 200 \text{ (Reduces)} =$ 6.400 archivos de shuffle.Con 32 particiones: $32 \text{ (Maps)} \times 32 \text{ (Reduces)} =$ 1.024 archivos de shuffle.Reducir a 32 particiones evita que el clúster cree, escriba y luego transfiera por red 5.376 archivos innecesarios, aliviando drásticamente el I/O del disco y la congestión de red durante el MERGE.

## Pregunta 5 — Benchmark Athena

Según `benchmark_resultados.md`: ¿cuál fue el ratio real de bytes
escaneados (CSV vs. Parquet)? ¿Por qué el ratio puede ser distinto del
teórico (~9x del slide de S4)? ¿Qué efecto tuvo el Z-ordering sobre los
bytes escaneados?

→ 
1. Ratio real obtenido:

"El ratio real de bytes escaneados en mi ejecución fue de 25.91x (775,366 bytes en CSV frente a solo 29,930 bytes en Parquet). Este valor superó con creces el orden de magnitud teórico de ~9x."

2. ¿Por qué difiere del teórico y qué efecto tuvo el Z-ordering?

"El multiplicador teórico de ~9x generalmente contempla solo dos ventajas base de Parquet: la compresión (ej. Snappy) y el formato columnar (Athena solo lee las columnas region, fecha y ventas_totales, ignorando el resto).

Sin embargo, el salto espectacular a 25.91x se le atribuye directamente al efecto del Z-ordering combinado con la selectividad de la query. Al aplicar ZORDER BY (fecha, region) en la capa Gold, ordenamos físicamente las filas dentro de los archivos para que las fechas similares quedaran agrupadas.

Como nuestra consulta en Athena tenía un filtro temporal (WHERE fecha >= date_add(...)), el motor aprovechó las estadísticas de mínimos y máximos en los metadatos de Parquet para hacer Data Skipping: pudo ignorar (saltarse) bloques enteros de datos viejos sin siquiera leerlos. Por el contrario, el CSV es un formato basado en filas y sin metadatos, lo que obligó a Athena a hacer un Full Table Scan absoluto (leer todas las filas y todas las columnas de los 10.000 registros) para poder evaluar la cláusula WHERE."
