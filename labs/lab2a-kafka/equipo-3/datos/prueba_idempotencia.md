# Prueba de idempotencia — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 31 de agosto de 2026
**Estudiantes:** Hellen Yanes Doria, Sebastian Salazar Henao, Andres
Felipe Velez Alvarez, Samuel Samper Cardona —
Hyanesd@eafit.edu.co, Ssalazarh3@eafit.edu.co, Afveleza@eafit.edu.co,
Ssamperc@eafit.edu.co

## Los 5 pasos

1. **Ejecuté el consumidor** — arrancó procesando la partición 0 del
   topic (recién recreado y con un productor nuevo de 1.000 mensajes).
2. **Lo detuve de forma abrupta** — el `Ctrl+C` normal quedó bloqueado
   por un error interno de `py4j` (`reentrant call inside
   BufferedReader`), así que tuve que matarlo a la fuerza desde otra
   terminal con `kill -9` mientras seguía procesando mensajes. Esto es,
   de hecho, una interrupción más abrupta y realista que un Ctrl+C
   limpio — el proceso murió sin ninguna oportunidad de terminar nada
   a medias.
3. **Conté los registros en Bronze**: **N = 109**.
4. **Reinicié el consumidor** — Kafka reenvió mensajes ya procesados
   antes de la caída (offsets 0 al ~30 de la partición 1), confirmando
   que no se habían commiteado antes del `kill -9`.
5. **Conté los registros otra vez**: **N' = 144** (subió porque, además
   de reprocesar mensajes viejos, el consumidor también avanzó con
   mensajes nuevos de otras particiones mientras corría). Como N ≠ N'
   no prueba nada por sí solo en este escenario, corrí la prueba
   definitiva: comparar el total de filas contra el total de
   `pedido_id` distintos.

## Evidencia — log del consumidor (antes de detener)

```
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

26/08/31 13:47:03 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large.
[OK] offset=0 partition=1 pedido_id=4c081524-2a5f-41ab-8e65-0a9394f21820
[OK] offset=1 partition=1 pedido_id=a63a29fc-d894-42d6-bdb9-2f4a8038b58c
[OK] offset=2 partition=1 pedido_id=c2dba54b-961e-4553-aa1f-a26c94974491
[OK] offset=3 partition=1 pedido_id=ce9f1cec-4810-4b67-9d1a-7279e87ca290
[OK] offset=4 partition=1 pedido_id=2718f7ec-2cfc-4e11-ae63-478652c5310a
...
[OK] offset=29 partition=1 pedido_id=b2a18908-ad17-4037-97a2-cdcc50c4fc52
[kill -9 aplicado externamente en este punto, offset ~108 de la ejecución
completa contando todas las particiones -- ver ps aux / kill -9 PID]
```

Verificación de que el proceso quedó muerto antes de contar Bronze:
```bash
$ ps aux | grep consumidor_kafka
usuario    95108  0.0  0.0   4256  2304 pts/4    S+   13:45   0:00 grep --color=auto consumidor_kafka
```
(solo aparece el propio `grep`, ningún proceso real corriendo)

## Evidencia — conteo de Bronze ANTES de reiniciar

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
print(spark.read.format("delta").load("/tmp/lake/bronze/pedidos").count())
```
```
N = 109
```

## Evidencia — log del consumidor (al reiniciar)

```
Ctrl+C para detener (útil para la prueba de idempotencia -- Parte 2.4 del README).

26/08/31 13:47:03 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large.
[OK] offset=0 partition=1 pedido_id=4c081524-2a5f-41ab-8e65-0a9394f21820
[OK] offset=1 partition=1 pedido_id=a63a29fc-d894-42d6-bdb9-2f4a8038b58c
[OK] offset=2 partition=1 pedido_id=c2dba54b-961e-4553-aa1f-a26c94974491
[OK] offset=3 partition=1 pedido_id=ce9f1cec-4810-4b67-9d1a-7279e87ca290
[OK] offset=4 partition=1 pedido_id=2718f7ec-2cfc-4e11-ae63-478652c5310a
[OK] offset=5 partition=1 pedido_id=a1143a00-815e-47a3-bb1e-1066551a4d25
[OK] offset=6 partition=1 pedido_id=75ccb353-c22f-4bc0-b0ac-7351ac9924bc
[OK] offset=7 partition=1 pedido_id=89d99b49-e7ff-4ba7-826f-bdcb4e6bdd0e
[OK] offset=8 partition=1 pedido_id=18cde00f-a484-401f-aaec-d6c13a312bc8
[OK] offset=9 partition=1 pedido_id=d14a87d1-4c83-459f-a717-22846d33c725
```

El offset=0 de la partición 1 reaparece exactamente con el mismo
`pedido_id` (`4c081524-2a5f-41ab-8e65-0a9394f21820`) que ya se había
visto y procesado en la corrida anterior — confirmación directa de
que Kafka reentregó un mensaje ya procesado, tal como se espera de
at-least-once con `enable_auto_commit=False`.

## Evidencia — conteo de Bronze DESPUÉS de reiniciar

```
N' = 144
```

## Evidencia — prueba definitiva (total vs. distintos)

Como N ≠ N' no es concluyente por sí solo en este escenario (Bronze
también creció por mensajes genuinamente nuevos, no solo por
reproceso), corrí la comparación que sí prueba idempotencia sin
ambigüedad:

```python
df = spark.read.format("delta").load("/tmp/lake/bronze/pedidos")
total = df.count()
distintos = df.select("pedido_id").distinct().count()
print(f"Total filas: {total}")
print(f"Pedido_id distintos: {distintos}")
print(f"¿Hay duplicados? {'SÍ' if total != distintos else 'NO'}")
```
```
Total filas: 144
Pedido_id distintos: 144
¿Hay duplicados? NO
```

## Interpretación

¿`N` es igual a `N'`? → **No** (109 → 144), pero por una razón
esperada y no problemática: el consumidor, al reiniciar, no solo
reprocesó mensajes ya vistos (offsets 0-30 de la partición 1, ya
contados en el N=109 original), sino que también procesó mensajes
genuinamente nuevos de otras particiones que nunca se habían
consumido. Por eso N vs N' no es la comparación correcta para probar
idempotencia en este escenario específico de prueba.

La comparación que sí lo prueba de forma concluyente es
**total de filas vs. `pedido_id` distintos**: ambos dieron **144**,
exactamente iguales. Esto demuestra, con el dataset completo de
Bronze (no solo con los mensajes que alcancé a observar en pantalla),
que **ningún** `pedido_id` quedó duplicado — ni siquiera los que
Kafka reentregó tras el `kill -9` a mitad de proceso.

**Conclusión:** el `MERGE ... ON pedido_id` es idempotente y mi
implementación de at-least-once funciona como se espera. Kafka puede
(y en esta prueba, de hecho lo hizo) reentregar mensajes ya
procesados tras una caída abrupta del consumidor, pero el `MERGE`
absorbe esa redundancia sin generar duplicados en Bronze — la
combinación de "Kafka puede duplicar la entrega" + "el MERGE es
idempotente" da como resultado neto una semántica de
exactamente-una-vez en el destino final, aunque el mecanismo de
entrega en tránsito solo garantice at-least-once.
