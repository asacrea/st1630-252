# Análisis CAP — WordCount con Spark

**Curso:** ST1630-2026-2 · **Semana:** S3 · **Fecha de entrega:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Copia este archivo a tu carpeta de entrega y complétalo. Úsalo solo si
> prefieres trabajar el análisis CAP en Markdown puro en vez de dentro
> del notebook (Celda 11 de `wordcount_reseñas.ipynb`) — no hace falta
> entregar ambos, elige uno.

## a) Operaciones del pipeline y su tipo de shuffle

Completa la tabla con lo que observaste en Spark UI (pestaña
SQL/DataFrame) al correr tu job. No copies un número "esperado" —
reporta lo que tu ejecución realmente mostró.

| Operación | Tipo (shuffle / no-shuffle) | ¿Exchange en el plan físico? (sí/no) |
|---|---|---|
| `explode` (tokenización) | | |
| `split` | | |
| `lower` | | |
| `filter` (stopwords y vacíos) | | |
| `groupBy` | | |
| `count` (agregación) | | |
| `orderBy` | | |

## b) Pregunta 1 — Conteo de shuffles

¿Cuántos nodos `Exchange` tiene el job completo (con `orderBy`) en
Spark UI? ¿Y el job simplificado (sin `orderBy`)? Indica el número
exacto de cada uno.

> → [tu respuesta aquí]

## c) Pregunta 2 — RAM vs. disco

Para cada shuffle que identificaste: ¿por qué Spark elige mantenerlo en
RAM en vez de escribir a disco como haría MapReduce clásico entre map y
reduce? ¿Qué garantiza el linaje del DAG que hace posible esa elección
si un executor falla a mitad de camino?

> → [tu respuesta aquí]

## d) Pregunta 3 — Clasificación CP/AP

Clasifica el WordCount de Spark como CP o AP según el Teorema CAP.
Justifica en 3–5 líneas **citando el comportamiento del shuffle que
observaste en Spark UI** (no solo la definición de teoría).

> → [tu respuesta aquí]

## e) Pregunta bonus — ¿Cuándo se parecería más a CP?

¿En qué condición el WordCount de Spark se comportaría más como un
sistema CP? (pista: piensa en cuándo Spark hace *spill* a disco).

> → [tu respuesta aquí]
