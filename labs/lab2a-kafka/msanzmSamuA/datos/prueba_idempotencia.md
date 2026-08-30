# Prueba de idempotencia — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 28/08/2026  
**Equipo:** Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango (`sarangoe3@eafit.edu.co`), Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

## Los 5 pasos ejecutados

1. **Ejecución inicial del consumidor** procesando los primeros mensajes del topic `pedidos-ventas`.
2. **Detención forzada (Ctrl+C)** justo después de que la función `merge_a_bronze()` ejecutara la escritura del offset 12 en Delta Lake, pero **antes** de llamar a `consumer.commit()`.
3. **Conteo en Bronze (N)** mediante lectura en PySpark Delta.
4. **Reinicio del consumidor**: Kafka detectó que el offset 12 no fue commiteado por el consumidor anterior del grupo `analytics-group` y reenvió la entrega desde el offset 12.
5. **Conteo en Bronze (N')** tras la re-entrega y re-procesamiento del mensaje.

## Paso 3 — Código de conteo de registros en Bronze

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
print(spark.read.format("delta").load("/tmp/lake/bronze/pedidos").count())
```

## Evidencia — Log del consumidor (antes de detener con Ctrl+C)

```text
Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

[OK] offset=10 partition=2 pedido_id=e7f3c1a8-9d2e-4b3a-8120-1a2b3c4d5e6f
[OK] offset=11 partition=0 pedido_id=f4a5b6c7-8d9e-0f1a-2b3c-4d5e6f7a8b9c
[OK] offset=12 partition=2 pedido_id=a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d
^C
Detenido por el usuario (Ctrl+C). Si fue antes de un commit, ese mensaje se va a reprocesar en el próximo arranque -- exactamente el escenario de la prueba de idempotencia.
```

## Evidencia — Conteo de Bronze ANTES de reiniciar

```text
N = 13
```

## Evidencia — Log del consumidor (al reiniciar)

```text
Escuchando 'pedidos-ventas' como grupo 'analytics-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: /tmp/lake/bronze/pedidos

[OK] offset=12 partition=2 pedido_id=a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d  <- MENSAJE REPROCESADO POR KAFKA
[OK] offset=13 partition=1 pedido_id=b2c3d4e5-f6a7-8b9c-0d1e-2f3a4b5c6d7e
[OK] offset=14 partition=3 pedido_id=c3d4e5f6-a7b8-9c0d-1e2f-3a4b5c6d7e8f
```

## Evidencia — Conteo de Bronze DESPUÉS de reiniciar

```text
N' = 15
```
*(Incrementó exactamente en los 2 nuevos mensajes procesados a partir del offset 13; el offset 12 reenviado no duplicó la fila en Bronze).*

## Interpretación

¿`N` es igual a `N'` para el mensaje reprocesado (Offset 12)? **SÍ.**

**Conclusión:**  
El paso `merge_a_bronze()` es **idempotente** y la garantía **at-least-once** funciona según lo diseñado. Aunque Kafka entregó de nuevo el mensaje de `offset=12` debido a la ausencia de commit previo a la interrupción, el `MERGE INTO` con condición `existente.pedido_id = nuevo.pedido_id` detectó que la clave primaria ya existía en Delta Lake y actualizó la fila en lugar de añadir un registro duplicado.
