# Análisis CAP — WordCount con Spark (ejemplo de referencia)

**Curso:** ST1630-2026-2 · **Semana:** S3 · **Fecha de entrega:** 2026-07-30
**Estudiante:** Ana Gómez (ana.gomez@eafit.edu.co) — **estudiante ficticio, ejemplo de referencia**

## a) Operaciones del pipeline y su tipo de shuffle

| Operación | Tipo (shuffle / no-shuffle) | ¿Exchange en el plan físico? (sí/no) |
|---|---|---|
| `explode` (tokenización) | No-shuffle — transformación *narrow*, cada línea produce N filas sin salir de su partición original | No |
| `split` | No-shuffle — se ejecuta antes del `explode`, sobre cada fila de forma independiente | No |
| `lower` | No-shuffle — función pura fila a fila | No |
| `filter` (stopwords y vacíos) | No-shuffle — cada partición filtra sus propias filas sin comunicarse con otras | No |
| `groupBy` | Shuffle — necesita reunir todas las filas con la misma `palabra` en la misma partición | Sí — 1 `Exchange hashpartitioning` |
| `count` (agregación) | Se apoya en el mismo shuffle del `groupBy`: `HashAggregate` parcial *antes* del Exchange (pre-agrega por partición) y `HashAggregate` final *después* | No agrega un Exchange nuevo — reusa el del `groupBy` |
| `orderBy` | Shuffle — necesita un reparticionamiento por rango para garantizar orden global entre particiones | Sí — 1 `Exchange rangepartitioning` adicional |

## b) Pregunta 1 — Conteo de shuffles

En mi ejecución (Databricks Community Edition, cluster de 1 nodo), el
job completo (`groupBy` + `count` + `orderBy`, Celda 7) mostró **2 nodos
`Exchange`** en la pestaña SQL de Spark UI: uno `hashpartitioning(palabra,
200)` justo antes del `HashAggregate` final, y otro `rangepartitioning`
para el `orderBy` final. El job simplificado (Celda 9, sin `orderBy`)
mostró **1 solo `Exchange`** — el mismo `hashpartitioning` de la
agregación, sin el segundo shuffle de ordenamiento. La diferencia de 1
Exchange es exactamente el costo de pedir un orden global sobre todo el
dataset en vez de solo agrupar y contar.

## c) Pregunta 2 — RAM vs. disco

Spark mantiene los datos del shuffle en buffers en memoria de cada
executor (con spill a disco solo si el buffer se llena, ver bonus) en
vez de escribir obligatoriamente a disco entre cada etapa como hace
MapReduce entre `map` y `reduce`. Puede permitirse esto porque cada
RDD/DataFrame que participa en el shuffle conserva su **linaje**: el
grafo de transformaciones (`explode` → `split` → `lower` → `filter` →
`groupBy`) que generó cada partición. Si un executor muere a mitad del
shuffle y se pierde una partición que solo vivía en RAM, Spark no
necesita un checkpoint en disco para recuperarla: vuelve a ejecutar,
desde la fuente de datos original, únicamente las transformaciones que
producían esa partición perdida, usando el DAG como "receta"
reproducible. MapReduce, en cambio, materializa cada resultado
intermedio en disco precisamente porque no tiene ese linaje
reconstruible: su única garantía de recuperación es la copia física
que ya escribió.

## d) Pregunta 3 — Clasificación CP/AP

Clasifico el WordCount de Spark como **AP**. En Spark UI, los dos
`Exchange` que observé (el `hashpartitioning` del `groupBy`/`count` y
el `rangepartitioning` del `orderBy`) mueven datos entre executors
usando buffers en memoria administrados por el *shuffle manager*: el
job no se detiene a esperar una confirmación de escritura durable en
disco antes de seguir, como sí ocurriría si Spark priorizara
Consistencia estricta sobre Disponibilidad ante una partición de red o
la caída de un executor. Ante la pérdida de una partición, Spark
prefiere **seguir respondiendo** reconstruyendo esa partición desde el
linaje del DAG (tal como describí en la Pregunta 2), en vez de
bloquear todo el job hasta poder garantizar que el dato nunca se
perdió — que es la postura CP que adopta HDFS/MapReduce al escribir
cada etapa a disco antes de continuar.

## e) Pregunta bonus — ¿Cuándo se parecería más a CP?

El WordCount de Spark se acerca más al comportamiento CP cuando ocurre
*spill* a disco durante el shuffle: si el volumen de datos de una
partición supera la memoria disponible del executor
(`spark.memory.fraction` / el buffer de shuffle se llena), Spark
empieza a escribir bloques del shuffle a disco de forma similar a
MapReduce, sacrificando algo de disponibilidad/latencia a cambio de no
perder el dato solo por falta de RAM. Otro caso equivalente es cuando
se usa `.checkpoint()` explícitamente: al truncar el linaje y forzar
una escritura durable a disco (o HDFS), Spark deja de poder reconstruir
esa partición recomputando el DAG, y en cambio depende — como
MapReduce — de que la copia física en disco esté disponible.
