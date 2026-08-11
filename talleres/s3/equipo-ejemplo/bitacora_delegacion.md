# Bitácora de delegación — Ana Gómez (ejemplo de referencia)

**Curso:** ST1630-2026-2 · **Semana:** S3 · **Fecha de entrega:** 2026-07-30
**Estudiante:** Ana Gómez (ana.gomez@eafit.edu.co) — **estudiante ficticio, ejemplo de referencia**

Ver `../../../docs/politica-ia.md` para la política completa. Esta
bitácora sigue el formato de esa política, aplicado a las tareas
específicas del taller S3.

| Tarea | ¿Delegado a agente? | Herramienta | Justificación |
|---|---|---|---|
| Instalar dependencias / resolver errores de setup | Sí | Claude Code | Mi cluster de Databricks Community quedó en estado "Terminated" a mitad de sesión (Error #4 del troubleshooting) y no recordaba el flujo exacto para reatacharlo al notebook. Es troubleshooting repetitivo de bajo valor de aprendizaje — la política lo permite delegar por defecto. |
| Adaptar las stopwords a mi criterio | Parcial | Claude Code | Le pedí al agente que sugiriera palabras de dominio (e-commerce) candidatas a stopword además de las 20 gramaticales de `STOPWORDS_ES`. El agente sugirió "producto" y "compra"; yo decidí NO agregarlas porque quería ver esas palabras en el top 20 para discutir en la Pregunta 3. La decisión final de qué excluir fue mía. |
| Escribir el análisis CAP (preguntas 1–3 + bonus) | No | — | Es el entregable diferenciador del taller (ver `../README.md`). Redacté las respuestas yo misma a partir de lo que vi en Spark UI; solo le pedí al agente que revisara ortografía y claridad de redacción una vez terminado el contenido, no que generara el argumento. |
| Interpretar el DAG en Spark UI | No | — | Conté los nodos `Exchange` y leí los tipos de partitioning (`hashpartitioning`, `rangepartitioning`) directamente desde la pestaña SQL de Spark UI de mi propia ejecución. Un agente no tiene acceso a mi Spark UI real, así que cualquier número que hubiera "sugerido" habría sido inventado. |
| Resolver las variaciones de la Parte 6 | Parcial | Claude Code | Para la variación de bigramas (6a) le pedí al agente el boilerplate de `F.array` sobre la lista de palabras por línea (sintaxis que no recordaba); yo adapté ese boilerplate a mi propio pipeline y decidí cómo definir el bigrama (palabras consecutivas dentro de la misma reseña, no cruzando reseñas). Las variaciones 6b y 6c las escribí sin ayuda porque eran variaciones directas del pipeline de la Parte 2. |

## Nota sobre honestidad de la bitácora

Nótese que no todo está marcado como "delegado" ni todo como "hecho a
mano": eso sería sospechoso en cualquier entrega real. Lo que hace
"completa" a esta bitácora (según la rúbrica de `../README.md`) es que
cada fila tiene una justificación **específica a esta ejecución**
(qué error concreto, qué sugerencia concreta) y no una frase genérica
como "el agente me ayudó con todo" o "no usé IA en nada".
