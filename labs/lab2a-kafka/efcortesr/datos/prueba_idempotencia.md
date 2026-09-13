# Prueba de idempotencia - Lab 2a

**Curso:** ST1630-2026-2 - **Semana:** S6-S7 - **Fecha:** 2026-08-26  
**Estudiante:** Emmanuel Cortes, Sara Hurtado, Juan Jose Osorio, Mariana Sanchez

## Configuracion de la prueba

Para repetir exactamente el mismo mensaje use una asignacion manual de particion y offset:

```powershell
$env:GROUP_ID='idempotencia-p2-group'
$env:BRONZE_PATH='C:\Users\Emman\st1630-252\data\local\lake\bronze\pedidos_idempotencia_controlada'
$env:ASSIGN_PARTITION='2'
$env:START_OFFSET='0'
$env:MAX_MESSAGES='1'
$env:BATCH_SIZE='1'
```

El mensaje probado fue `pedidos-ventas`, particion `2`, offset `0`.

## Evidencia - log del consumidor antes de reiniciar

Se ejecuto el consumidor con `FAIL_AFTER_MERGE_ON_OFFSET=0`, que fuerza un fallo despues del MERGE Delta y antes del commit:

```text
Escuchando 'pedidos-ventas' como grupo 'idempotencia-p2-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: C:\Users\Emman\st1630-252\data\local\lake\bronze\pedidos_idempotencia_controlada
Asignacion manual: partition=2 start_offset=0
Modo prueba: el consumidor se detendra tras 1 mensajes procesados.

[ERROR] offset=0 partition=2 no se commiteo -- se reprocesara. Causa: Fallo controlado despues del MERGE y antes del commit

Procesados: 0  Rechazados (sin commit): 1
```

## Evidencia - conteo de Bronze ANTES de reiniciar

```text
BRONZE_PATH=C:\Users\Emman\st1630-252\data\local\lake\bronze\pedidos_idempotencia_controlada
COUNT=1
+----------------+-----+
|_kafka_partition|count|
+----------------+-----+
|2               |1    |
+----------------+-----+
```

Entonces:

```text
N = 1
```

## Evidencia - log del consumidor al reiniciar

Se reinicio el mismo grupo, misma particion y mismo offset, sin `FAIL_AFTER_MERGE_ON_OFFSET`:

```text
Escuchando 'pedidos-ventas' como grupo 'idempotencia-p2-group' (bootstrap: localhost:9092)...
Escribiendo a Bronze en: C:\Users\Emman\st1630-252\data\local\lake\bronze\pedidos_idempotencia_controlada
Asignacion manual: partition=2 start_offset=0
Modo prueba: el consumidor se detendra tras 1 mensajes procesados.

[OK] offset=0 partition=2 pedido_id=93dc8117-d207-4884-9d47-f795555fdb91 commit=1

Procesados: 1  Rechazados (sin commit): 0
```

## Evidencia - conteo de Bronze DESPUES de reiniciar

```text
BRONZE_PATH=C:\Users\Emman\st1630-252\data\local\lake\bronze\pedidos_idempotencia_controlada
COUNT=1
+----------------+-----+
|_kafka_partition|count|
+----------------+-----+
|2               |1    |
+----------------+-----+
```

Entonces:

```text
N' = 1
```

## Interpretacion

`N = N' = 1`. El mismo mensaje fue procesado dos veces: la primera vez se hizo el MERGE pero no se commiteo el offset; la segunda vez Kafka lo entrego otra vez y el consumidor commiteo `offset + 1`.

Bronze no duplico la fila porque el MERGE usa `pedido_id` como condicion de idempotencia:

```text
existente.pedido_id = nuevo.pedido_id
```

Esto confirma la garantia at-least-once: Kafka puede volver a entregar mensajes no commiteados, pero el destino Delta mantiene un unico registro logico por pedido.
