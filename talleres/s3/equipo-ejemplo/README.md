# Ejemplo de entrega — Ana Gómez (referencia de calidad)

**Curso:** ST1630-2026-2 · **Semana:** S3 · **Última edición:** 2026-07-30

> Esta carpeta **no es una plantilla para copiar y pegar**. Es un
> ejemplo de una entrega completa y de buena calidad, con un estudiante
> ficticio ("Ana Gómez"), para que puedas calibrar qué tan profundo debe
> ser tu propio análisis antes de entregarlo. Compara tus respuestas
> contra estas en forma, no en contenido — tus números de Spark UI y tus
> conclusiones deben salir de **tu** ejecución, no de esta.

## Archivos de este ejemplo

- [`cap_analysis_ejemplo.md`](cap_analysis_ejemplo.md) — análisis CAP
  completo (tabla + preguntas 1–3 + bonus), con justificaciones que
  citan evidencia concreta de Spark UI en vez de repetir la teoría de
  memoria.
- [`bitacora_delegacion.md`](bitacora_delegacion.md) — bitácora de
  delegación con decisiones justificadas fila por fila, ni todo
  delegado ni nada delegado sin razón.

Lo que falta en esta carpeta (`wordcount_reseñas.ipynb` ejecutado) se
omite intencionalmente: cada estudiante debe generar sus propios
outputs de Spark UI, que varían levemente según el cluster y la
partición por defecto — copiar un notebook ejecutado ajeno no
demuestra haber corrido nada.

## Qué hace que este ejemplo sea "completo" y no solo "relleno"

Según la rúbrica de `../README.md`:

- El análisis CAP cita **números concretos** (cuántos `Exchange`, qué
  tipo de partitioning) en vez de decir genéricamente "hay shuffles".
- La clasificación CP/AP no se queda en la definición de libro: conecta
  el comportamiento observado (el shuffle vive en buffers en memoria de
  los executors, no se escribe a disco salvo spill) con la garantía que
  lo hace seguro (el linaje del DAG permite recomputar una partición
  perdida sin bloquear las demás).
- La bitácora no marca todo como "no delegado" por quedar bien: declara
  con honestidad qué se delegó y por qué, siguiendo
  `../../../docs/politica-ia.md`.
