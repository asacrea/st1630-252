# Prueba de idempotencia — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 30/08/2026
**Estudiante:** Sebastian Andres Medina Cabezas - samedinac@eafit.edu.co

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
Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

[OK] offset=15 partition=0 pedido_id=901                                        
[OK] offset=16 partition=0 pedido_id=903                                        
[OK] offset=17 partition=0 pedido_id=904                                        

Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.
```

## Evidencia — conteo de Bronze ANTES de reiniciar

```
N = 21
```

## Evidencia — log del consumidor (al reiniciar)

```
Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

[OK] offset=17 partition=0 pedido_id=904                                        
[OK] offset=18 partition=0 pedido_id=999                                        

Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.
```

## Evidencia — conteo de Bronze DESPUÉS de reiniciar

```
N' = 22
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

→ Aunque el inicio (N=21) y el final (N'=22) no son iguales, esta diferencia de un solo mensaje es lo que demuestra el éxito de la idempotencia en mi implementación. Al reiniciar el consumidor tras la simulación de la caída, Kafka cumplió reenviando el mensaje con el offset=17 (cuyo commit había sido interrumpido) junto con un mensaje nuevo correspondiente al offset=18. Si el sistema no tuviera protección contra duplicados, la base de datos habría guardado ambos mensajes, elevando el total a 23. Sin embargo, al quedar el conteo final en 22, se comprueba que la operación MERGE en la capa Bronze de Delta Lake reconoció el pedido_id duplicado del offset 17 y evito insertarlo de nuevo.
