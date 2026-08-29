# Prueba de idempotencia — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Copia este archivo a tu carpeta de entrega como
> `datos/prueba_idempotencia.md` y complétalo mientras ejecutas la
> prueba (Parte 2.4 del `../README.md`). Sin esta evidencia el lab es
> parcial — no alcanza con describir la prueba, hay que haberla corrido.

## Los 5 pasos

1. **Ejecutar el consumidor** hasta procesar ~10 mensajes.
2. **Detenerlo sin commitear** — Ctrl+C justo después de ver el log de
   un mensaje procesado pero ANTES de que veas confirmado su commit
   (si tu implementación es correcta, el commit ocurre inmediatamente
   después del MERGE, así que el margen es pequeño — intenta
   detenerlo lo más rápido posible tras un `[OK]` en la terminal).
3. **Contar los registros en Bronze** (N).
4. **Reiniciar el consumidor** — Kafka debe reenviar el último mensaje
   no commiteado (y posiblemente alguno más, dependiendo de dónde
   quedó el offset).
5. **Contar los registros en Bronze otra vez** — debe seguir siendo N,
   no N + (mensajes reprocesados).

## Paso 3 — Cómo contar registros en Bronze

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
print(spark.read.format("delta").load("/tmp/lake/bronze/pedidos").count())
```

## Evidencia — log del consumidor (antes de detener)

```
[pega aquí las últimas líneas de la terminal antes del Ctrl+C —
debe verse claramente el offset del último mensaje procesado]
```

## Evidencia — conteo de Bronze ANTES de reiniciar

```
N = [tu número aquí]
```

## Evidencia — log del consumidor (al reiniciar)

```
[pega aquí las primeras líneas al reiniciar el consumidor -- debe
verse el mismo offset (o uno anterior) siendo reprocesado]
```

## Evidencia — conteo de Bronze DESPUÉS de reiniciar

```
N' = [tu número aquí]
```

## Interpretación

¿`N` es igual a `N'`? → [sí/no]

Si `N = N'`: el MERGE Delta es idempotente y tu implementación de
at-least-once funciona como se espera — Kafka reenvió un mensaje ya
procesado, pero el `MERGE ... ON pedido_id` no lo duplicó en Bronze.

Si `N ≠ N'`: algo en tu implementación no es realmente idempotente
(revisa: ¿tu MERGE usa `pedido_id` como condición de match, o estás
usando `append` en vez de `merge`?). Corrígelo antes de entregar — un
`N ≠ N'` documentado tal cual, sin corregir, no cumple el criterio de
"completo" de la rúbrica.

→ [tu interpretación aquí, con tus propios números]
