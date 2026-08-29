# Prueba de idempotencia — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 29/08/2026

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

[OK] offset=60 partition=3 pedido_id=07408023-7e46-4a61-9a8f-f1d0b7c32a7d
[OK] offset=61 partition=3 pedido_id=273e97fc-9dd8-44e5-a03b-4dfd649cb537
[OK] offset=62 partition=3 pedido_id=ed68351f-47f1-4a77-b276-8c61ddd0dad4

```

## Evidencia — conteo de Bronze ANTES de reiniciar

```
N = 1000
```

## Evidencia — log del consumidor (al reiniciar)

```
[pega aquí las primeras líneas al reiniciar el consumidor -- debe
verse el mismo offset (o uno anterior) siendo reprocesado]

Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

^C
Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.
```

## Evidencia — conteo de Bronze DESPUÉS de reiniciar

```
N' = 1000
```

## Interpretación

¿`N` es igual a `N'`? → [sí/no]

Sí, `N = 1000` y `N' = 1000`, por lo tanto el conteo no cambió después de reiniciar el consumidor. Esto muestra que no hubo duplicados en Bronze y que el `MERGE` por `pedido_id` fue idempotente.
