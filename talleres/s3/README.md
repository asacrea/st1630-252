# Taller S3 — WordCount con análisis CAP: primer job Spark real

**Curso:** ST1630-2026-2 · **Semana:** S3 · **Última edición:** 2026-07-30
**Duración:** 2 h · **Estatus:** formativo — **no calificado**

> Este taller no puntúa dentro del 30% de laboratorios (ver
> `../../docs/evaluacion.md`). Es un ensayo del método exacto que usarán
> los labs calificados desde S4: notebook + evidencia + análisis + PR.
> Corran el formato completo ahora, sin presión de nota, para llegar
> entrenados al Lab 1b (S5).

## Objetivo

Ejecutar un job real de Apache Spark (WordCount sobre reseñas de
producto) y conectar cada shuffle observado en Spark UI con la posición
de Spark en el Teorema CAP (Brewer, 2000): un sistema AP que sostiene
esa elección gracias al linaje del DAG, en contraste con la escritura a
disco de MapReduce clásico (una decisión CP).

Al terminar, cada estudiante debe poder responder sin dudar: **"¿por
qué Spark puede permitirse mantener el shuffle en RAM en vez de
escribir a disco como MapReduce, y qué pasa si un executor falla
mientras tanto?"**

## Prerequisitos

- Cuenta activa en [Databricks Community Edition](https://community.cloud.databricks.com)
  (gratuita; regístrense con su correo @eafit.edu.co si es posible).
- Python 3.9+ si van a probar el notebook en local antes de subirlo
  (opcional — el flujo principal es Databricks).
- Repositorio del curso clonado desde S1, con este taller (`talleres/s3/`)
  actualizado (`git pull`).
- Haber leído la sección de CAP del deck de teoría de S3 (Brewer 2000,
  Gilbert & Lynch 2002 — ver `recursos.md`).

## Los 6 pasos del lab (2:00 h)

| Tiempo | Paso | Qué producen |
|---|---|---|
| 0:00–0:15 | 1. Setup del cluster y carga de datos | Cluster Databricks corriendo, dataset cargado en DBFS o leído en local |
| 0:15–0:45 | 2. Transformaciones (plan lazy) | Pipeline de tokenización + stopwords, sin ejecutar aún |
| 0:45–1:05 | 3. Acción — ejecutar el DAG | `.show(20)` disparado, resultados del WordCount visibles |
| 1:05–1:35 | 4. Análisis del DAG en Spark UI | Capturas o notas de los Exchange (shuffles) del job completo vs. el simplificado |
| 1:35–1:55 | 5. Análisis CAP (entregable principal) | Tabla operación→shuffle + preguntas 1–3 + bonus, completas |
| 1:55–2:00 | 6. Extensión y cierre | Al menos 1 de las 3 variaciones de la Parte 6 intentada, PR abierto |

### Paso 1 · Setup del cluster y carga de datos (0:00–0:15)

1. Entren a Databricks Community Edition → **Compute** → **Create
   compute** (cluster mono-nodo, runtime por defecto está bien).
2. Esperen a que el cluster quede en estado **Running** (ícono verde).
   Esto toma 3–5 minutos — es el momento de leer la Parte 1 del
   notebook mientras esperan.
3. Suban `datos/reseñas_producto.txt` a DBFS: **Catalog → Upload File**
   (o `dbutils.fs.cp` si prefieren notebook-driven upload), o simplemente
   abran el notebook desde el repo y ajusten la ruta si trabajan en
   local con PySpark instalado (`pip install pyspark`).
4. Abran `notebook_base/wordcount_reseñas.ipynb`, adjúntenlo al
   cluster, y ejecuten la Celda 3.

**Verifiquen:** `.count()` debe reportar 302 líneas leídas (incluye
líneas vacías y de comentario — eso es intencional, se filtran después).

**Error frecuente:** `Path does not exist` al leer el archivo. La ruta
cambia según dónde ejecuten:
- Local: `spark.read.text("../datos/reseñas_producto.txt")`
- Databricks (DBFS): `spark.read.text("dbfs:/FileStore/reseñas_producto.txt")`

Ambas rutas están comentadas en la Celda 3 del notebook — descomenten
la que corresponda a su entorno.

### Paso 2 · Transformaciones — plan lazy (0:15–0:45)

1. Ejecuten la Celda 5: construye el pipeline de tokenización
   (`explode` + `split` + `lower`) y el filtro de stopwords.
2. Observen que la celda **no imprime ningún resultado** — solo define
   el plan. Esto es evaluación perezosa: Spark construye el DAG pero no
   ejecuta nada hasta que llega una acción.
3. Revisen la lista `STOPWORDS_ES` y discutan en pareja: ¿alguna
   palabra del dominio (e-commerce, reseñas) debería agregarse aunque
   no sea una stopword gramatical clásica? (p. ej. "producto" aparece
   tanto que puede ensuciar el ranking).

**Verifiquen:** la celda corre sin error y sin output visible más allá
de la referencia al DataFrame (`DataFrame[...]`) — si ven resultados de
conteo aquí, algo se está ejecutando antes de tiempo (probable
`.show()` o `.count()` colado en esta celda).

**Error frecuente:** `AnalysisException: cannot resolve column` — casi
siempre es un nombre de columna mal escrito después del `explode`
(revisen el `alias()` que le dieron a la columna de palabras).

### Paso 3 · Acción — ejecutar el DAG (0:45–1:05)

1. Ejecuten la Celda 7: `groupBy` + `count` + `orderBy` + `.show(20)`.
2. **Este es el momento en que todo el plan lazy se ejecuta de una
   vez.** Spark UI empieza a llenarse de datos justo aquí.
3. Ejecuten `.explain()` y ubiquen en el Physical Plan las palabras
   `Exchange` (shuffle) y `HashAggregate` (agregación local antes y
   después del shuffle).

**Verifiquen:** el top de palabras debe estar dominado por artículos y
conectores (el, la, muy, calidad, entrega...) si no filtraron
stopwords correctamente, o por sustantivos de dominio (calidad,
entrega, producto...) si el filtro funcionó.

**Error frecuente:** el conteo top 20 sigue mostrando "el", "la", "de"
arriba de todo — revisen que el `filter()` de stopwords se aplique
**después** del `lower()` (comparar en minúsculas contra minúsculas).

### Paso 4 · Análisis del DAG en Spark UI (1:05–1:35)

1. Ejecuten la Celda 9 (segundo job, sin `orderBy`) para tener dos
   jobs que comparar.
2. Naveguen a Spark UI: **Compute → (su cluster) → Spark UI → pestaña
   SQL / DataFrame**. Ahí verán una entrada por cada acción ejecutada
   (una para el job con `orderBy`, otra para el simplificado).
3. Abran el job del `groupBy + count + orderBy` y cuenten cuántos
   nodos **Exchange** aparecen en el gráfico del plan. Repitan con el
   job simplificado (sin `orderBy`).
4. Anoten el número de cada uno — lo van a necesitar en la Parte 5.

**Verifiquen:** el job completo (con `orderBy`) debe tener **más**
Exchange que el simplificado, porque `orderBy` requiere un shuffle
adicional (reparticionamiento total para el ordenamiento global) que
`groupBy` + `count` no necesita repetir.

**Error frecuente:** no encuentran la pestaña Spark UI — en Databricks
Community está dentro del detalle del cluster (**Compute → clic en el
nombre del cluster → pestaña "Spark UI"**), no en el menú lateral
general.

### Paso 5 · Análisis CAP — entregable principal (1:35–1:55)

1. Completen la Celda 11 del notebook (o `plantillas/cap_analysis_template.md`
   si prefieren Markdown puro fuera del notebook).
2. Llenen la tabla operación→shuffle citando lo que vieron en el Paso 4
   (no lo que "deberían" haber visto — el número real de su ejecución).
3. Respondan las 3 preguntas + el bonus. Este es el diferenciador del
   lab: conecta la teoría de CAP con evidencia empírica de Spark UI.

**Verifiquen (auto-chequeo antes de entregar):** su respuesta a la
Pregunta 3 debe **citar explícitamente** algo que vieron en Spark UI
(un número de Exchange, un nombre de nodo del plan), no solo repetir la
definición de teoría de memoria.

**Error frecuente:** confundir "Spark es AP" con "Spark nunca falla".
AP no significa infalible — significa que ante una partición/falla,
Spark prioriza seguir respondiendo (reconstruyendo desde el linaje) en
vez de bloquear la operación hasta garantizar consistencia total, que
es lo que haría un sistema CP.

### Paso 6 · Extensión y cierre (1:55–2:00)

1. Intenten al menos **una** de las 3 variaciones de la Celda 13
   (bigramas, filtro por "entrega", promedio de palabras por reseña).
2. Completen la bitácora de delegación (Celda 15 del notebook).
3. Abran el Pull Request antes de que cierre la sesión.

## Entregable — estructura del PR

Suban su carpeta de entrega siguiendo el patrón de `equipo-ejemplo/`
(ver esa carpeta como referencia de calidad, con el estudiante ficticio
Ana Gómez). Cada estudiante entrega su propia carpeta:

```
talleres/s3/
└── entregas/
    └── <su-usuario-o-nombre>/
        ├── README.md                     # copiado de equipo-ejemplo/README.md
        ├── wordcount_reseñas.ipynb       # notebook ejecutado, con outputs visibles
        ├── cap_analysis.md               # o la Celda 11 completa dentro del notebook
        └── bitacora_delegacion.md        # o la Celda 15 completa dentro del notebook
```

No es obligatorio duplicar el análisis CAP en archivo aparte si ya
está completo dentro del notebook (Celda 11) — elijan un solo lugar y
sean consistentes. Abran el PR hacia `main` con título
`taller(s3): <su nombre> — wordcount + análisis CAP`.

## Rúbrica de revisión (formativa — no afecta la nota)

| Criterio | Completo | Parcial | Incompleto |
|---|---|---|---|
| **Ejecución del pipeline** | El notebook corre de principio a fin sin errores, con outputs visibles del `.show()` y `.explain()` | Corre pero con algún paso saltado o output incompleto (p. ej. `.explain()` faltante) | No corre, o el estudiante pegó outputs de otra ejecución sin correrlo ellos mismos |
| **Evidencia de Spark UI** | Reporta el número exacto de Exchange de ambos jobs (con/sin `orderBy`) y por qué difieren | Reporta el número pero no explica la diferencia entre los dos jobs | No hay evidencia de haber abierto Spark UI (números inventados o ausentes) |
| **Análisis CAP** | Las 3 preguntas + bonus están respondidas citando evidencia concreta del propio job; clasificación CP/AP correcta y bien justificada | Respuestas presentes pero genéricas (repiten teoría sin conectar con su ejecución), o falta el bonus | Preguntas sin responder, respuestas incorrectas (p. ej. clasifican Spark como CP), o copiadas literalmente de la teoría sin adaptar |
| **Bitácora de delegación** | Completa, con justificación específica por fila (no genérica) y consistente con lo que realmente se delegó | Completa pero con justificaciones vagas ("me ayudó con todo") | Ausente, o marca como "no delegado" tareas que evidentemente sí lo fueron |

## Bitácora de delegación

Este taller sigue `../../docs/politica-ia.md`. Recordatorio rápido de
qué aplica en este lab específico:

**Se puede delegar por defecto:**
- Troubleshooting de instalación/configuración del cluster Databricks.
- Boilerplate de PySpark (imports, estructura de la SparkSession).
- Dudas puntuales de sintaxis (`explode`, `split`, `groupBy`, etc.).
- Generar variaciones adicionales de stopwords o de las variaciones de
  la Celda 13, siempre que el estudiante entienda y pueda explicar el
  resultado.

**Debe hacerse a mano por defecto:**
- La lectura del Spark UI y el conteo de shuffles (Paso 4) — es
  observación directa, no algo que un agente pueda inventar sin ver la
  UI real.
- El análisis CAP (Parte 5, preguntas 1–3 + bonus) — es el entregable
  diferenciador del lab; un agente puede ayudar a pulir redacción, pero
  la clasificación CP/AP y su justificación deben reflejar el
  razonamiento propio del estudiante.
- La interpretación de por qué el job con `orderBy` tiene más shuffles
  que el simplificado.

Como este taller **no es calificado**, no hay penalización formal por
delegación no declarada — pero declárenla igual: es la práctica que
van a necesitar desde el Lab 1b (S5), donde sí cuenta.

## Troubleshooting

| # | Error / síntoma | Causa probable | Solución |
|---|---|---|---|
| 1 | `Path does not exist: file:/...reseñas_producto.txt` | Ruta relativa incorrecta o archivo no subido a DBFS | En Databricks usen `dbfs:/FileStore/reseñas_producto.txt` (después de subirlo por Catalog → Upload File); en local, verifiquen que están ejecutando desde `notebook_base/` para que `../datos/...` resuelva |
| 2 | El top de palabras sigue mostrando "el", "la", "de", "que" | El `filter()` de stopwords no se aplicó, o se comparó contra la columna original en vez de la ya minuscularizada | Verifiquen que el filtro use la misma columna que salió de `.lower()`, y que `STOPWORDS_ES` esté en minúsculas |
| 3 | `AnalysisException: Column 'word' does not exist` (o similar) | El `alias()` de la columna tras `explode`/`split` no coincide con el nombre usado después | Revisen que el nombre en `.alias("word")` sea exactamente el que usan en `groupBy("word")` |
| 4 | El cluster de Databricks Community se apaga o queda "Terminated" a mitad de sesión | Los clusters Community Edition se auto-terminan tras inactividad (~2 h) o por límite de la cuenta gratuita | Reinicien el cluster desde **Compute**, reatáchenlo al notebook, y vuelvan a correr desde la Celda 3 (el estado en memoria se pierde) |
| 5 | Spark UI no muestra ningún job | Se está mirando la pestaña antes de ejecutar una acción (`.show()`, `.count()`), o se abrió la UI del cluster equivocado | Ejecuten primero la acción (Celda 7 o 9) y refresquen Spark UI; confirmen que están en el cluster que tienen adjuntado al notebook |
| 6 | `UnicodeDecodeError` o caracteres raros (Ã±, Ã©) al leer el dataset | El archivo se subió o se leyó con un encoding distinto a UTF-8 | Aseguren que la subida a DBFS preserve UTF-8; en local, `spark.read.text()` usa UTF-8 por defecto — si editaron el archivo con otra herramienta, revisen que lo hayan guardado como UTF-8 |
| 7 | El número de Exchange que ven en Spark UI no coincide con lo que "debería" ser según la teoría | Cada ejecución puede variar levemente según el AQE (Adaptive Query Execution) o el número de particiones por defecto del cluster | Reporten el número **real** que observan, no el "esperado" — es parte del ejercicio notar que el plan físico depende del entorno de ejecución |

## Referencias

Ver `recursos.md` para el listado completo con enlaces. Lecturas
mínimas para este taller: Brewer (2000) sobre CAP y Zaharia et al.
(2012) sobre el modelo de RDDs y linaje en Spark.
