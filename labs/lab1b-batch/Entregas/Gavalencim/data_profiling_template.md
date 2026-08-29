# Data Profiling — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 19/08/2026
**Estudiante:** Emmanuel Alvarez Castrillon --- ealvarezc1@eafit.edu.co

> Copia este archivo a tu carpeta de entrega como `data_profiling.md` y
> complétalo **después** de ejecutar `scripts/00_profiling.py` (Parte 1
> del lab), **antes** de tocar nada del pipeline Silver. Pega el output
> relevante del script junto a cada respuesta — no solo la respuesta
> final.

## 1. Duplicados exactos

¿Cuántos duplicados exactos tiene el dataset?

Duplicados exactos: 1,500 (1.48%)

```

=== Filas totales: 101,500 ===
26/08/20 01:01:43 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/20 01:01:43 INFO CodeGenerator: Code generated in 74.482206 ms
26/08/20 01:01:43 INFO DAGScheduler: Registering RDD 26 (count at NativeMethodAccessorImpl.java:0) as input to shuffle 1
26/08/20 01:01:43 INFO DAGScheduler: Got map stage job 3 (count at NativeMethodAccessorImpl.java:0) with 4 output partitions
26/08/20 01:01:43 INFO DAGScheduler: Final stage: ShuffleMapStage 4 (count at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:43 INFO DAGScheduler: Parents of final stage: List()
26/08/20 01:01:43 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:43 INFO DAGScheduler: Submitting ShuffleMapStage 4 (MapPartitionsRDD[26] at count at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:43 INFO MemoryStore: Block broadcast_6 stored as values in memory (estimated size 39.9 KiB, free 911.8 MiB)
26/08/20 01:01:43 INFO MemoryStore: Block broadcast_6_piece0 stored as bytes in memory (estimated size 16.5 KiB, free 911.7 MiB)
26/08/20 01:01:43 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 16.5 KiB, free: 912.2 MiB)
26/08/20 01:01:43 INFO SparkContext: Created broadcast 6 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:43 INFO DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 4 (MapPartitionsRDD[26] at count at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
26/08/20 01:01:43 INFO YarnScheduler: Adding task set 4.0 with 4 tasks resource profile 0
26/08/20 01:01:43 INFO TaskSetManager: Starting task 0.0 in stage 4.0 (TID 6) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:43 INFO TaskSetManager: Starting task 1.0 in stage 4.0 (TID 7) (ip-10-0-1-171.ec2.internal, executor 1, partition 1, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:43 INFO TaskSetManager: Starting task 2.0 in stage 4.0 (TID 8) (ip-10-0-1-171.ec2.internal, executor 1, partition 2, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:43 INFO TaskSetManager: Starting task 3.0 in stage 4.0 (TID 9) (ip-10-0-1-171.ec2.internal, executor 1, partition 3, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:43 INFO BlockManagerInfo: Added broadcast_6_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 16.5 KiB, free: 4.8 GiB)
26/08/20 01:01:44 INFO TaskSetManager: Finished task 3.0 in stage 4.0 (TID 9) in 822 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/4)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 0.0 in stage 4.0 (TID 6) in 1188 ms on ip-10-0-1-171.ec2.internal (executor 1) (2/4)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 1.0 in stage 4.0 (TID 7) in 1215 ms on ip-10-0-1-171.ec2.internal (executor 1) (3/4)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 2.0 in stage 4.0 (TID 8) in 1261 ms on ip-10-0-1-171.ec2.internal (executor 1) (4/4)
26/08/20 01:01:45 INFO YarnScheduler: Removed TaskSet 4.0, whose tasks have all completed, from pool
26/08/20 01:01:45 INFO DAGScheduler: ShuffleMapStage 4 (count at NativeMethodAccessorImpl.java:0) finished in 1.278 s
26/08/20 01:01:45 INFO DAGScheduler: looking for newly runnable stages
26/08/20 01:01:45 INFO DAGScheduler: running: Set()
26/08/20 01:01:45 INFO DAGScheduler: waiting: Set()
26/08/20 01:01:45 INFO DAGScheduler: failed: Set()
26/08/20 01:01:45 INFO ShufflePartitionsUtil: For shuffle(1), advisory target size: 67108864, actual target size 7738780, minimum partition size: 1048576
26/08/20 01:01:45 INFO ShufflePartitionsUtil: For shuffle(1), advisory target size: 67108864, actual target size 7738780, minimum partition size: 1048576
26/08/20 01:01:45 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/20 01:01:45 INFO CodeGenerator: Code generated in 39.998673 ms
26/08/20 01:01:45 INFO DAGScheduler: Registering RDD 29 (count at NativeMethodAccessorImpl.java:0) as input to shuffle 2
26/08/20 01:01:45 INFO DAGScheduler: Got map stage job 4 (count at NativeMethodAccessorImpl.java:0) with 5 output partitions
26/08/20 01:01:45 INFO DAGScheduler: Final stage: ShuffleMapStage 6 (count at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:45 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 5)
26/08/20 01:01:45 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:45 INFO DAGScheduler: Submitting ShuffleMapStage 6 (MapPartitionsRDD[29] at count at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:45 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 25.0 KiB, free 911.7 MiB)
26/08/20 01:01:45 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 10.1 KiB, free 911.7 MiB)
26/08/20 01:01:45 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 10.1 KiB, free: 912.2 MiB)
26/08/20 01:01:45 INFO SparkContext: Created broadcast 7 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:45 INFO DAGScheduler: Submitting 5 missing tasks from ShuffleMapStage 6 (MapPartitionsRDD[29] at count at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4))
26/08/20 01:01:45 INFO YarnScheduler: Adding task set 6.0 with 5 tasks resource profile 0
26/08/20 01:01:45 INFO TaskSetManager: Starting task 0.0 in stage 6.0 (TID 10) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, NODE_LOCAL, 7363 bytes)
26/08/20 01:01:45 INFO TaskSetManager: Starting task 1.0 in stage 6.0 (TID 11) (ip-10-0-1-171.ec2.internal, executor 1, partition 1, NODE_LOCAL, 7363 bytes)
26/08/20 01:01:45 INFO TaskSetManager: Starting task 2.0 in stage 6.0 (TID 12) (ip-10-0-1-171.ec2.internal, executor 1, partition 2, NODE_LOCAL, 7363 bytes)
26/08/20 01:01:45 INFO TaskSetManager: Starting task 3.0 in stage 6.0 (TID 13) (ip-10-0-1-171.ec2.internal, executor 1, partition 3, NODE_LOCAL, 7363 bytes)
26/08/20 01:01:45 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 10.1 KiB, free: 4.8 GiB)
26/08/20 01:01:45 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 1 to 10.0.1.171:47070
26/08/20 01:01:45 INFO TaskSetManager: Starting task 4.0 in stage 6.0 (TID 14) (ip-10-0-1-171.ec2.internal, executor 1, partition 4, NODE_LOCAL, 7363 bytes)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 0.0 in stage 6.0 (TID 10) in 297 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/5)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 2.0 in stage 6.0 (TID 12) in 302 ms on ip-10-0-1-171.ec2.internal (executor 1) (2/5)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 3.0 in stage 6.0 (TID 13) in 316 ms on ip-10-0-1-171.ec2.internal (executor 1) (3/5)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 1.0 in stage 6.0 (TID 11) in 338 ms on ip-10-0-1-171.ec2.internal (executor 1) (4/5)
26/08/20 01:01:45 INFO TaskSetManager: Finished task 4.0 in stage 6.0 (TID 14) in 51 ms on ip-10-0-1-171.ec2.internal (executor 1) (5/5)
26/08/20 01:01:45 INFO YarnScheduler: Removed TaskSet 6.0, whose tasks have all completed, from pool
26/08/20 01:01:45 INFO DAGScheduler: ShuffleMapStage 6 (count at NativeMethodAccessorImpl.java:0) finished in 0.359 s
26/08/20 01:01:45 INFO DAGScheduler: looking for newly runnable stages
26/08/20 01:01:45 INFO DAGScheduler: running: Set()
26/08/20 01:01:45 INFO DAGScheduler: waiting: Set()
26/08/20 01:01:45 INFO DAGScheduler: failed: Set()
26/08/20 01:01:45 INFO CodeGenerator: Code generated in 12.364084 ms
26/08/20 01:01:45 INFO SparkContext: Starting job: count at NativeMethodAccessorImpl.java:0
26/08/20 01:01:45 INFO DAGScheduler: Got job 5 (count at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/20 01:01:45 INFO DAGScheduler: Final stage: ResultStage 9 (count at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:45 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 8)
26/08/20 01:01:45 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:45 INFO DAGScheduler: Submitting ResultStage 9 (MapPartitionsRDD[32] at count at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:45 INFO MemoryStore: Block broadcast_8 stored as values in memory (estimated size 12.9 KiB, free 911.7 MiB)
26/08/20 01:01:45 INFO MemoryStore: Block broadcast_8_piece0 stored as bytes in memory (estimated size 6.2 KiB, free 911.7 MiB)
26/08/20 01:01:45 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 6.2 KiB, free: 912.2 MiB)
26/08/20 01:01:45 INFO SparkContext: Created broadcast 8 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:45 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 9 (MapPartitionsRDD[32] at count at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/20 01:01:45 INFO YarnScheduler: Adding task set 9.0 with 1 tasks resource profile 0
26/08/20 01:01:45 INFO TaskSetManager: Starting task 0.0 in stage 9.0 (TID 15) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, NODE_LOCAL, 7374 bytes)
26/08/20 01:01:45 INFO BlockManagerInfo: Added broadcast_8_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 6.2 KiB, free: 4.8 GiB)
26/08/20 01:01:45 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 2 to 10.0.1.171:47070
26/08/20 01:01:45 INFO TaskSetManager: Finished task 0.0 in stage 9.0 (TID 15) in 44 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/1)
26/08/20 01:01:45 INFO YarnScheduler: Removed TaskSet 9.0, whose tasks have all completed, from pool
26/08/20 01:01:45 INFO DAGScheduler: ResultStage 9 (count at NativeMethodAccessorImpl.java:0) finished in 0.052 s
26/08/20 01:01:45 INFO DAGScheduler: Job 5 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/20 01:01:45 INFO YarnScheduler: Killing all running tasks in stage 9: Stage finished
26/08/20 01:01:45 INFO DAGScheduler: Job 5 finished: count at NativeMethodAccessorImpl.java:0, took 0.059962 s
Duplicados exactos: 1,500 (1.48%)
```

## 2. Formatos de fecha

¿Cuántos formatos de fecha distintos puedes identificar? Lista al
menos 3 con ejemplos reales del dataset (valores tal cual aparecen en
la columna `fecha`).

Hay 4 formatos de fecha distintos:


|patron_fecha                     |count|
| --------- | -----: |
|dd/MM/yyyy o MM/dd/yyyy (ambiguo)|40644|
|yyyy/MM/dd                       |20293|
|yyyy-MM-dd                       |20284|
|dd-MM-yyyy                       |20279|

```

=== Formatos de fecha detectados (top 10 por patrón) ===
26/08/20 01:01:47 INFO CodeGenerator: Code generated in 65.894779 ms
26/08/20 01:01:47 INFO DAGScheduler: Registering RDD 45 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 4
26/08/20 01:01:47 INFO DAGScheduler: Got map stage job 8 (showString at NativeMethodAccessorImpl.java:0) with 4 output partitions
26/08/20 01:01:47 INFO DAGScheduler: Final stage: ShuffleMapStage 13 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:47 INFO DAGScheduler: Parents of final stage: List()
26/08/20 01:01:47 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:47 INFO DAGScheduler: Submitting ShuffleMapStage 13 (MapPartitionsRDD[45] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:47 INFO MemoryStore: Block broadcast_11 stored as values in memory (estimated size 46.5 KiB, free 911.5 MiB)
26/08/20 01:01:47 INFO MemoryStore: Block broadcast_11_piece0 stored as bytes in memory (estimated size 21.2 KiB, free 911.5 MiB)
26/08/20 01:01:47 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 21.2 KiB, free: 912.2 MiB)
26/08/20 01:01:47 INFO SparkContext: Created broadcast 11 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:47 INFO DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 13 (MapPartitionsRDD[45] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
26/08/20 01:01:47 INFO YarnScheduler: Adding task set 13.0 with 4 tasks resource profile 0
26/08/20 01:01:47 INFO TaskSetManager: Starting task 0.0 in stage 13.0 (TID 21) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:47 INFO TaskSetManager: Starting task 1.0 in stage 13.0 (TID 22) (ip-10-0-1-171.ec2.internal, executor 1, partition 1, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:47 INFO TaskSetManager: Starting task 2.0 in stage 13.0 (TID 23) (ip-10-0-1-171.ec2.internal, executor 1, partition 2, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:47 INFO TaskSetManager: Starting task 3.0 in stage 13.0 (TID 24) (ip-10-0-1-171.ec2.internal, executor 1, partition 3, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:47 INFO BlockManagerInfo: Added broadcast_11_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 21.2 KiB, free: 4.8 GiB)
26/08/20 01:01:47 INFO TaskSetManager: Finished task 3.0 in stage 13.0 (TID 24) in 239 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/4)
26/08/20 01:01:47 INFO TaskSetManager: Finished task 1.0 in stage 13.0 (TID 22) in 324 ms on ip-10-0-1-171.ec2.internal (executor 1) (2/4)
26/08/20 01:01:47 INFO TaskSetManager: Finished task 2.0 in stage 13.0 (TID 23) in 328 ms on ip-10-0-1-171.ec2.internal (executor 1) (3/4)
26/08/20 01:01:47 INFO TaskSetManager: Finished task 0.0 in stage 13.0 (TID 21) in 344 ms on ip-10-0-1-171.ec2.internal (executor 1) (4/4)
26/08/20 01:01:47 INFO YarnScheduler: Removed TaskSet 13.0, whose tasks have all completed, from pool
26/08/20 01:01:47 INFO DAGScheduler: ShuffleMapStage 13 (showString at NativeMethodAccessorImpl.java:0) finished in 0.360 s
26/08/20 01:01:47 INFO DAGScheduler: looking for newly runnable stages
26/08/20 01:01:47 INFO DAGScheduler: running: Set()
26/08/20 01:01:47 INFO DAGScheduler: waiting: Set()
26/08/20 01:01:47 INFO DAGScheduler: failed: Set()
26/08/20 01:01:47 INFO ShufflePartitionsUtil: For shuffle(4), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:01:47 INFO ShufflePartitionsUtil: For shuffle(4), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:01:47 INFO CodeGenerator: Code generated in 12.498711 ms
26/08/20 01:01:47 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/20 01:01:47 INFO CodeGenerator: Code generated in 19.032202 ms
26/08/20 01:01:47 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
26/08/20 01:01:47 INFO DAGScheduler: Got job 9 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/20 01:01:47 INFO DAGScheduler: Final stage: ResultStage 15 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:47 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 14)
26/08/20 01:01:47 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:47 INFO DAGScheduler: Submitting ResultStage 15 (MapPartitionsRDD[49] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:47 INFO MemoryStore: Block broadcast_12 stored as values in memory (estimated size 28.5 KiB, free 911.5 MiB)
26/08/20 01:01:47 INFO MemoryStore: Block broadcast_12_piece0 stored as bytes in memory (estimated size 13.2 KiB, free 911.5 MiB)
26/08/20 01:01:47 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 13.2 KiB, free: 912.1 MiB)
26/08/20 01:01:47 INFO SparkContext: Created broadcast 12 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:47 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 15 (MapPartitionsRDD[49] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/20 01:01:47 INFO YarnScheduler: Adding task set 15.0 with 1 tasks resource profile 0
26/08/20 01:01:47 INFO TaskSetManager: Starting task 0.0 in stage 15.0 (TID 25) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, NODE_LOCAL, 7374 bytes)
26/08/20 01:01:47 INFO BlockManagerInfo: Added broadcast_12_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 13.2 KiB, free: 4.8 GiB)
26/08/20 01:01:47 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 4 to 10.0.1.171:47070
26/08/20 01:01:47 INFO TaskSetManager: Finished task 0.0 in stage 15.0 (TID 25) in 144 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/1)
26/08/20 01:01:47 INFO YarnScheduler: Removed TaskSet 15.0, whose tasks have all completed, from pool
26/08/20 01:01:47 INFO DAGScheduler: ResultStage 15 (showString at NativeMethodAccessorImpl.java:0) finished in 0.158 s
26/08/20 01:01:47 INFO DAGScheduler: Job 9 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/20 01:01:47 INFO YarnScheduler: Killing all running tasks in stage 15: Stage finished
26/08/20 01:01:47 INFO DAGScheduler: Job 9 finished: showString at NativeMethodAccessorImpl.java:0, took 0.164690 s
26/08/20 01:01:47 INFO CodeGenerator: Code generated in 13.330461 ms
26/08/20 01:01:47 INFO CodeGenerator: Code generated in 7.307983 ms
+---------------------------------+-----+
|patron_fecha                     |count|
+---------------------------------+-----+
|dd/MM/yyyy o MM/dd/yyyy (ambiguo)|40644|
|yyyy/MM/dd                       |20293|
|yyyy-MM-dd                       |20284|
|dd-MM-yyyy                       |20279|
+---------------------------------+-----+

```

## 3. Variantes de "Bogotá"

¿Cuántas variantes de "Bogotá" existen en la columna `region`? Lístalas
todas con su conteo.

Hay 8 variantes de Bogotá:

|  # | Variante  | Conteo |
| -: | --------- | -----: |
|  1 | `BOGOTÁ`  |  5,017 |
|  2 | `Bogota`  |  4,956 |
|  3 | `bogota`  |  4,894 |
|  4 | `BTA`     |  4,803 |
|  5 | `Bta`     |  4,796 |
|  6 | `BOGOTA`  |  4,759 |
|  7 | ` Bogotá` |  4,701 |
|  8 | `Bogotá`  |  4,677 |


```

=== Valores únicos de 'region' (ordenados por frecuencia) ===
26/08/20 01:01:48 INFO CodeGenerator: Code generated in 33.684913 ms
26/08/20 01:01:48 INFO DAGScheduler: Registering RDD 54 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 5
26/08/20 01:01:48 INFO DAGScheduler: Got map stage job 10 (showString at NativeMethodAccessorImpl.java:0) with 4 output partitions
26/08/20 01:01:48 INFO DAGScheduler: Final stage: ShuffleMapStage 16 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:48 INFO DAGScheduler: Parents of final stage: List()
26/08/20 01:01:48 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:48 INFO DAGScheduler: Submitting ShuffleMapStage 16 (MapPartitionsRDD[54] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:48 INFO MemoryStore: Block broadcast_13 stored as values in memory (estimated size 43.8 KiB, free 911.4 MiB)
26/08/20 01:01:48 INFO MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 20.1 KiB, free 911.4 MiB)
26/08/20 01:01:48 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 20.1 KiB, free: 912.1 MiB)
26/08/20 01:01:48 INFO SparkContext: Created broadcast 13 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:48 INFO DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 16 (MapPartitionsRDD[54] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
26/08/20 01:01:48 INFO YarnScheduler: Adding task set 16.0 with 4 tasks resource profile 0
26/08/20 01:01:48 INFO TaskSetManager: Starting task 0.0 in stage 16.0 (TID 26) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:48 INFO TaskSetManager: Starting task 1.0 in stage 16.0 (TID 27) (ip-10-0-1-171.ec2.internal, executor 1, partition 1, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:48 INFO TaskSetManager: Starting task 2.0 in stage 16.0 (TID 28) (ip-10-0-1-171.ec2.internal, executor 1, partition 2, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:48 INFO TaskSetManager: Starting task 3.0 in stage 16.0 (TID 29) (ip-10-0-1-171.ec2.internal, executor 1, partition 3, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:48 INFO BlockManagerInfo: Added broadcast_13_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 20.1 KiB, free: 4.8 GiB)
26/08/20 01:01:48 INFO TaskSetManager: Finished task 3.0 in stage 16.0 (TID 29) in 127 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/4)
26/08/20 01:01:48 INFO TaskSetManager: Finished task 2.0 in stage 16.0 (TID 28) in 153 ms on ip-10-0-1-171.ec2.internal (executor 1) (2/4)
26/08/20 01:01:48 INFO TaskSetManager: Finished task 0.0 in stage 16.0 (TID 26) in 155 ms on ip-10-0-1-171.ec2.internal (executor 1) (3/4)
26/08/20 01:01:48 INFO TaskSetManager: Finished task 1.0 in stage 16.0 (TID 27) in 161 ms on ip-10-0-1-171.ec2.internal (executor 1) (4/4)
26/08/20 01:01:48 INFO YarnScheduler: Removed TaskSet 16.0, whose tasks have all completed, from pool
26/08/20 01:01:48 INFO DAGScheduler: ShuffleMapStage 16 (showString at NativeMethodAccessorImpl.java:0) finished in 0.185 s
26/08/20 01:01:48 INFO DAGScheduler: looking for newly runnable stages
26/08/20 01:01:48 INFO DAGScheduler: running: Set()
26/08/20 01:01:48 INFO DAGScheduler: waiting: Set()
26/08/20 01:01:48 INFO DAGScheduler: failed: Set()
26/08/20 01:01:48 INFO ShufflePartitionsUtil: For shuffle(5), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:01:48 INFO ShufflePartitionsUtil: For shuffle(5), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:01:48 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/20 01:01:48 INFO CodeGenerator: Code generated in 30.405749 ms
26/08/20 01:01:48 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
26/08/20 01:01:48 INFO DAGScheduler: Got job 11 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/20 01:01:48 INFO DAGScheduler: Final stage: ResultStage 18 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:48 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 17)
26/08/20 01:01:48 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:48 INFO DAGScheduler: Submitting ResultStage 18 (MapPartitionsRDD[58] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:48 INFO MemoryStore: Block broadcast_14 stored as values in memory (estimated size 28.9 KiB, free 911.4 MiB)
26/08/20 01:01:48 INFO MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 13.2 KiB, free 911.4 MiB)
26/08/20 01:01:48 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 13.2 KiB, free: 912.1 MiB)
26/08/20 01:01:48 INFO SparkContext: Created broadcast 14 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:48 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 18 (MapPartitionsRDD[58] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/20 01:01:48 INFO YarnScheduler: Adding task set 18.0 with 1 tasks resource profile 0
26/08/20 01:01:48 INFO TaskSetManager: Starting task 0.0 in stage 18.0 (TID 30) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, NODE_LOCAL, 7374 bytes)
26/08/20 01:01:48 INFO BlockManagerInfo: Added broadcast_14_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 13.2 KiB, free: 4.8 GiB)
26/08/20 01:01:48 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 5 to 10.0.1.171:47070
26/08/20 01:01:48 INFO TaskSetManager: Finished task 0.0 in stage 18.0 (TID 30) in 76 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/1)
26/08/20 01:01:48 INFO YarnScheduler: Removed TaskSet 18.0, whose tasks have all completed, from pool
26/08/20 01:01:48 INFO DAGScheduler: ResultStage 18 (showString at NativeMethodAccessorImpl.java:0) finished in 0.087 s
26/08/20 01:01:48 INFO DAGScheduler: Job 11 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/20 01:01:48 INFO YarnScheduler: Killing all running tasks in stage 18: Stage finished
26/08/20 01:01:48 INFO DAGScheduler: Job 11 finished: showString at NativeMethodAccessorImpl.java:0, took 0.096898 s
26/08/20 01:01:48 INFO CodeGenerator: Code generated in 11.502186 ms
26/08/20 01:01:48 INFO CodeGenerator: Code generated in 14.38118 ms
+------------+-----+
|region      |count|
+------------+-----+
|BOGOTÁ      |5017 |
|Bogota      |4956 |
|bogota      |4894 |
|BTA         |4803 |
|Bta         |4796 |
|BOGOTA      |4759 |
| Bogotá     |4701 |
|Bogotá      |4677 |
|Medellín    |3487 |
|MEDELLÍN    |3444 |
|medellin    |3425 |
|Medellin    |3392 |
|MDE         |3332 |
|medellín    |3316 |
|CALI        |2598 |
|Cali        |2579 |
| Cali       |2570 |
|CLO         |2550 |
|cali        |2487 |
|cali        |2473 |
|BARRANQUILLA|2042 |
|Bquilla     |2018 |
|Barranquilla|2015 |
|BAQ         |2015 |
|barranquilla|1912 |
|BGA         |1869 |
|Bucaramanga |1842 |
|Buca        |1837 |
|bucaramanga |1830 |
|BUCARAMANGA |1734 |
|Desconocido |1665 |
|otro        |1634 |
|N/A         |1632 |
|NA          |1615 |
|OTRO        |1584 |
+------------+-----+
```

## 4. Variantes de "app_movil"

¿Cuántas variantes de "app_movil" existen en la columna `canal`?
Lístalas todas con su conteo.

Las variantes de app_movil son 5:

| Variante    | Conteo |
| ----------- | -----: |
| `App Móvil` |  7,198 |
| `móvil`     |  7,158 |
| `app movil` |  7,121 |
| `APP MOVIL` |  7,090 |
| `APP_MOVIL` |  7,004 |


```

=== Valores únicos de 'canal' (ordenados por frecuencia) ===
26/08/20 01:01:49 INFO DAGScheduler: Registering RDD 74 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 8
26/08/20 01:01:49 INFO DAGScheduler: Got map stage job 15 (showString at NativeMethodAccessorImpl.java:0) with 4 output partitions
26/08/20 01:01:49 INFO DAGScheduler: Final stage: ShuffleMapStage 25 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:49 INFO DAGScheduler: Parents of final stage: List()
26/08/20 01:01:49 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:49 INFO DAGScheduler: Submitting ShuffleMapStage 25 (MapPartitionsRDD[74] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:49 INFO MemoryStore: Block broadcast_18 stored as values in memory (estimated size 43.8 KiB, free 911.2 MiB)
26/08/20 01:01:49 INFO MemoryStore: Block broadcast_18_piece0 stored as bytes in memory (estimated size 20.1 KiB, free 911.2 MiB)
26/08/20 01:01:49 INFO BlockManagerInfo: Added broadcast_18_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 20.1 KiB, free: 912.1 MiB)
26/08/20 01:01:49 INFO SparkContext: Created broadcast 18 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:49 INFO DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 25 (MapPartitionsRDD[74] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
26/08/20 01:01:49 INFO YarnScheduler: Adding task set 25.0 with 4 tasks resource profile 0
26/08/20 01:01:49 INFO TaskSetManager: Starting task 0.0 in stage 25.0 (TID 37) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:49 INFO TaskSetManager: Starting task 1.0 in stage 25.0 (TID 38) (ip-10-0-1-171.ec2.internal, executor 1, partition 1, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:49 INFO TaskSetManager: Starting task 2.0 in stage 25.0 (TID 39) (ip-10-0-1-171.ec2.internal, executor 1, partition 2, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:49 INFO TaskSetManager: Starting task 3.0 in stage 25.0 (TID 40) (ip-10-0-1-171.ec2.internal, executor 1, partition 3, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:49 INFO BlockManagerInfo: Added broadcast_18_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 20.1 KiB, free: 4.8 GiB)
26/08/20 01:01:49 INFO TaskSetManager: Finished task 0.0 in stage 25.0 (TID 37) in 93 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/4)
26/08/20 01:01:49 INFO TaskSetManager: Finished task 3.0 in stage 25.0 (TID 40) in 93 ms on ip-10-0-1-171.ec2.internal (executor 1) (2/4)
26/08/20 01:01:49 INFO TaskSetManager: Finished task 1.0 in stage 25.0 (TID 38) in 105 ms on ip-10-0-1-171.ec2.internal (executor 1) (3/4)
26/08/20 01:01:49 INFO TaskSetManager: Finished task 2.0 in stage 25.0 (TID 39) in 105 ms on ip-10-0-1-171.ec2.internal (executor 1) (4/4)
26/08/20 01:01:49 INFO YarnScheduler: Removed TaskSet 25.0, whose tasks have all completed, from pool
26/08/20 01:01:49 INFO DAGScheduler: ShuffleMapStage 25 (showString at NativeMethodAccessorImpl.java:0) finished in 0.121 s
26/08/20 01:01:49 INFO DAGScheduler: looking for newly runnable stages
26/08/20 01:01:49 INFO DAGScheduler: running: Set()
26/08/20 01:01:49 INFO DAGScheduler: waiting: Set()
26/08/20 01:01:49 INFO DAGScheduler: failed: Set()
26/08/20 01:01:49 INFO ShufflePartitionsUtil: For shuffle(8), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:01:49 INFO ShufflePartitionsUtil: For shuffle(8), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:01:49 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/20 01:01:49 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
26/08/20 01:01:49 INFO DAGScheduler: Got job 16 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/20 01:01:49 INFO DAGScheduler: Final stage: ResultStage 27 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:49 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 26)
26/08/20 01:01:49 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:49 INFO DAGScheduler: Submitting ResultStage 27 (MapPartitionsRDD[78] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:49 INFO MemoryStore: Block broadcast_19 stored as values in memory (estimated size 28.9 KiB, free 911.2 MiB)
26/08/20 01:01:49 INFO MemoryStore: Block broadcast_19_piece0 stored as bytes in memory (estimated size 13.2 KiB, free 911.2 MiB)
26/08/20 01:01:49 INFO BlockManagerInfo: Added broadcast_19_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 13.2 KiB, free: 912.1 MiB)
26/08/20 01:01:49 INFO SparkContext: Created broadcast 19 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:49 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 27 (MapPartitionsRDD[78] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/20 01:01:49 INFO YarnScheduler: Adding task set 27.0 with 1 tasks resource profile 0
26/08/20 01:01:49 INFO TaskSetManager: Starting task 0.0 in stage 27.0 (TID 41) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, NODE_LOCAL, 7374 bytes)
26/08/20 01:01:49 INFO BlockManagerInfo: Added broadcast_19_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 13.2 KiB, free: 4.8 GiB)
26/08/20 01:01:49 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 8 to 10.0.1.171:47070
26/08/20 01:01:49 INFO TaskSetManager: Finished task 0.0 in stage 27.0 (TID 41) in 36 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/1)
26/08/20 01:01:49 INFO YarnScheduler: Removed TaskSet 27.0, whose tasks have all completed, from pool
26/08/20 01:01:49 INFO DAGScheduler: ResultStage 27 (showString at NativeMethodAccessorImpl.java:0) finished in 0.047 s
26/08/20 01:01:49 INFO DAGScheduler: Job 16 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/20 01:01:49 INFO YarnScheduler: Killing all running tasks in stage 27: Stage finished
26/08/20 01:01:49 INFO DAGScheduler: Job 16 finished: showString at NativeMethodAccessorImpl.java:0, took 0.051148 s
+-------------+-----+
|canal        |count|
+-------------+-----+
|App Móvil    |7198 |
|móvil        |7158 |
|app movil    |7121 |
|APP MOVIL    |7090 |
|APP_MOVIL    |7004 |
|online       |6112 |
|pagina_web   |6111 |
|WEB          |6083 |
|sitio_web    |6082 |
|Web          |6036 |
|TIENDA FISICA|5118 |
|Tienda Física|5105 |
|tienda       |5065 |
|TIENDA       |4977 |
|físico       |4893 |
|call_center  |2181 |
|llamada      |2076 |
|TELEFONO     |2054 |
|tel          |2020 |
|Teléfono     |2016 |
+-------------+-----+
```

## 5. `total` <= 0 o nulo

¿Qué porcentaje de filas tiene `total <= 0` o nulo?

Hay 2571 nulos y son 101.500 filas por tanto representan el 3.9%

```

=== Estadísticas de 'total' ===
26/08/20 01:01:50 INFO BlockManagerInfo: Removed broadcast_10_piece0 on ip-10-0-1-171.ec2.internal:42953 in memory (size: 11.3 KiB, free: 4.8 GiB)
26/08/20 01:01:50 INFO BlockManagerInfo: Removed broadcast_10_piece0 on ip-10-0-1-54.ec2.internal:36275 in memory (size: 11.3 KiB, free: 912.2 MiB)
26/08/20 01:01:50 INFO BlockManagerInfo: Removed broadcast_19_piece0 on ip-10-0-1-54.ec2.internal:36275 in memory (size: 13.2 KiB, free: 912.2 MiB)
26/08/20 01:01:50 INFO BlockManagerInfo: Removed broadcast_19_piece0 on ip-10-0-1-171.ec2.internal:42953 in memory (size: 13.2 KiB, free: 4.8 GiB)
26/08/20 01:01:50 INFO BlockManagerInfo: Removed broadcast_14_piece0 on ip-10-0-1-54.ec2.internal:36275 in memory (size: 13.2 KiB, free: 912.2 MiB)
26/08/20 01:01:50 INFO BlockManagerInfo: Removed broadcast_14_piece0 on ip-10-0-1-171.ec2.internal:42953 in memory (size: 13.2 KiB, free: 4.8 GiB)
26/08/20 01:01:50 INFO CodeGenerator: Code generated in 34.02251 ms
26/08/20 01:01:50 INFO DAGScheduler: Registering RDD 94 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 11
26/08/20 01:01:50 INFO DAGScheduler: Got map stage job 20 (showString at NativeMethodAccessorImpl.java:0) with 4 output partitions
26/08/20 01:01:50 INFO DAGScheduler: Final stage: ShuffleMapStage 34 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:50 INFO DAGScheduler: Parents of final stage: List()
26/08/20 01:01:50 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:50 INFO DAGScheduler: Submitting ShuffleMapStage 34 (MapPartitionsRDD[94] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:50 INFO MemoryStore: Block broadcast_23 stored as values in memory (estimated size 35.8 KiB, free 911.7 MiB)
26/08/20 01:01:50 INFO MemoryStore: Block broadcast_23_piece0 stored as bytes in memory (estimated size 15.5 KiB, free 911.7 MiB)
26/08/20 01:01:50 INFO BlockManagerInfo: Added broadcast_23_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 15.5 KiB, free: 912.2 MiB)
26/08/20 01:01:50 INFO SparkContext: Created broadcast 23 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:50 INFO DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 34 (MapPartitionsRDD[94] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
26/08/20 01:01:50 INFO YarnScheduler: Adding task set 34.0 with 4 tasks resource profile 0
26/08/20 01:01:50 INFO TaskSetManager: Starting task 0.0 in stage 34.0 (TID 48) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:50 INFO TaskSetManager: Starting task 1.0 in stage 34.0 (TID 49) (ip-10-0-1-171.ec2.internal, executor 1, partition 1, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:50 INFO TaskSetManager: Starting task 2.0 in stage 34.0 (TID 50) (ip-10-0-1-171.ec2.internal, executor 1, partition 2, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:50 INFO TaskSetManager: Starting task 3.0 in stage 34.0 (TID 51) (ip-10-0-1-171.ec2.internal, executor 1, partition 3, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:01:50 INFO BlockManagerInfo: Added broadcast_23_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 15.5 KiB, free: 4.8 GiB)
26/08/20 01:01:50 INFO TaskSetManager: Finished task 3.0 in stage 34.0 (TID 51) in 115 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/4)
26/08/20 01:01:50 INFO TaskSetManager: Finished task 1.0 in stage 34.0 (TID 49) in 158 ms on ip-10-0-1-171.ec2.internal (executor 1) (2/4)
26/08/20 01:01:50 INFO TaskSetManager: Finished task 0.0 in stage 34.0 (TID 48) in 170 ms on ip-10-0-1-171.ec2.internal (executor 1) (3/4)
26/08/20 01:01:50 INFO TaskSetManager: Finished task 2.0 in stage 34.0 (TID 50) in 170 ms on ip-10-0-1-171.ec2.internal (executor 1) (4/4)
26/08/20 01:01:50 INFO DAGScheduler: ShuffleMapStage 34 (showString at NativeMethodAccessorImpl.java:0) finished in 0.182 s
26/08/20 01:01:50 INFO DAGScheduler: looking for newly runnable stages
26/08/20 01:01:50 INFO DAGScheduler: running: Set()
26/08/20 01:01:50 INFO DAGScheduler: waiting: Set()
26/08/20 01:01:50 INFO DAGScheduler: failed: Set()
26/08/20 01:01:50 INFO YarnScheduler: Removed TaskSet 34.0, whose tasks have all completed, from pool
26/08/20 01:01:50 INFO CodeGenerator: Code generated in 21.922633 ms
26/08/20 01:01:50 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
26/08/20 01:01:50 INFO DAGScheduler: Got job 21 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/20 01:01:50 INFO DAGScheduler: Final stage: ResultStage 36 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:01:50 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 35)
26/08/20 01:01:50 INFO DAGScheduler: Missing parents: List()
26/08/20 01:01:50 INFO DAGScheduler: Submitting ResultStage 36 (MapPartitionsRDD[97] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:01:50 INFO MemoryStore: Block broadcast_24 stored as values in memory (estimated size 23.1 KiB, free 911.7 MiB)
26/08/20 01:01:50 INFO MemoryStore: Block broadcast_24_piece0 stored as bytes in memory (estimated size 8.9 KiB, free 911.7 MiB)
26/08/20 01:01:50 INFO BlockManagerInfo: Added broadcast_24_piece0 in memory on ip-10-0-1-54.ec2.internal:36275 (size: 8.9 KiB, free: 912.2 MiB)
26/08/20 01:01:50 INFO SparkContext: Created broadcast 24 from broadcast at DAGScheduler.scala:1592
26/08/20 01:01:50 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 36 (MapPartitionsRDD[97] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/20 01:01:50 INFO YarnScheduler: Adding task set 36.0 with 1 tasks resource profile 0
26/08/20 01:01:50 INFO TaskSetManager: Starting task 0.0 in stage 36.0 (TID 52) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, NODE_LOCAL, 7374 bytes)
26/08/20 01:01:50 INFO BlockManagerInfo: Added broadcast_24_piece0 in memory on ip-10-0-1-171.ec2.internal:42953 (size: 8.9 KiB, free: 4.8 GiB)
26/08/20 01:01:50 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 11 to 10.0.1.171:47070
26/08/20 01:01:50 INFO TaskSetManager: Finished task 0.0 in stage 36.0 (TID 52) in 44 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/1)
26/08/20 01:01:50 INFO YarnScheduler: Removed TaskSet 36.0, whose tasks have all completed, from pool
26/08/20 01:01:50 INFO DAGScheduler: ResultStage 36 (showString at NativeMethodAccessorImpl.java:0) finished in 0.051 s
26/08/20 01:01:50 INFO DAGScheduler: Job 21 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/20 01:01:50 INFO YarnScheduler: Killing all running tasks in stage 36: Stage finished
26/08/20 01:01:50 INFO DAGScheduler: Job 21 finished: showString at NativeMethodAccessorImpl.java:0, took 0.054004 s
26/08/20 01:01:50 INFO CodeGenerator: Code generated in 8.07406 ms
+------------------+-------+----------------+-----+---------+-----+
|min               |max    |mean            |nulos|negativos|ceros|
+------------------+-------+----------------+-----+---------+-----+
|-49989.55707293571|3.893E9|3986873.59396779|2571 |926      |462  |
+------------------+-------+----------------+-----+---------+-----+
```

## 6. Tipo de dato de `vendedor_id`

¿Qué tipo de dato tiene la columna `vendedor_id`? ¿Es consistente en
todas las filas?

La columna vendedor_id está almacenada como texto (string), pero no es consistente en cuanto al formato de sus valores. Se encontraron 69.592 registros enteros, 28.056 con prefijo VEN- y 3.852 valores mixtos, por lo que será necesario normalizar o clasificar estos formatos durante la limpieza en Silver.

```


=== Tipos detectados en 'vendedor_id' ===
26/08/20 01:42:10 INFO CodeGenerator: Code generated in 24.917008 ms
26/08/20 01:42:10 INFO DAGScheduler: Registering RDD 118 (showString at NativeMethodAccessorImpl.java:0) as input to shuffle 14
26/08/20 01:42:10 INFO DAGScheduler: Got map stage job 26 (showString at NativeMethodAccessorImpl.java:0) with 4 output partitions
26/08/20 01:42:10 INFO DAGScheduler: Final stage: ShuffleMapStage 43 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:42:10 INFO DAGScheduler: Parents of final stage: List()
26/08/20 01:42:10 INFO DAGScheduler: Missing parents: List()
26/08/20 01:42:10 INFO DAGScheduler: Submitting ShuffleMapStage 43 (MapPartitionsRDD[118] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:42:10 INFO MemoryStore: Block broadcast_29 stored as values in memory (estimated size 45.5 KiB, free 911.4 MiB)
26/08/20 01:42:10 INFO MemoryStore: Block broadcast_29_piece0 stored as bytes in memory (estimated size 21.0 KiB, free 911.4 MiB)
26/08/20 01:42:10 INFO BlockManagerInfo: Added broadcast_29_piece0 in memory on ip-10-0-1-54.ec2.internal:33771 (size: 21.0 KiB, free: 912.1 MiB)
26/08/20 01:42:10 INFO SparkContext: Created broadcast 29 from broadcast at DAGScheduler.scala:1592
26/08/20 01:42:10 INFO DAGScheduler: Submitting 4 missing tasks from ShuffleMapStage 43 (MapPartitionsRDD[118] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0, 1, 2, 3))
26/08/20 01:42:10 INFO YarnScheduler: Adding task set 43.0 with 4 tasks resource profile 0
26/08/20 01:42:10 INFO TaskSetManager: Starting task 0.0 in stage 43.0 (TID 63) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:42:10 INFO TaskSetManager: Starting task 1.0 in stage 43.0 (TID 64) (ip-10-0-1-171.ec2.internal, executor 1, partition 1, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:42:10 INFO TaskSetManager: Starting task 2.0 in stage 43.0 (TID 65) (ip-10-0-1-171.ec2.internal, executor 1, partition 2, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:42:10 INFO TaskSetManager: Starting task 3.0 in stage 43.0 (TID 66) (ip-10-0-1-171.ec2.internal, executor 1, partition 3, PROCESS_LOCAL, 8047 bytes)
26/08/20 01:42:10 INFO BlockManagerInfo: Added broadcast_29_piece0 in memory on ip-10-0-1-171.ec2.internal:39119 (size: 21.0 KiB, free: 4.8 GiB)
26/08/20 01:42:10 INFO TaskSetManager: Finished task 3.0 in stage 43.0 (TID 66) in 58 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/4)
26/08/20 01:42:10 INFO TaskSetManager: Finished task 1.0 in stage 43.0 (TID 64) in 105 ms on ip-10-0-1-171.ec2.internal (executor 1) (2/4)
26/08/20 01:42:10 INFO TaskSetManager: Finished task 2.0 in stage 43.0 (TID 65) in 105 ms on ip-10-0-1-171.ec2.internal (executor 1) (3/4)
26/08/20 01:42:10 INFO TaskSetManager: Finished task 0.0 in stage 43.0 (TID 63) in 108 ms on ip-10-0-1-171.ec2.internal (executor 1) (4/4)
26/08/20 01:42:10 INFO YarnScheduler: Removed TaskSet 43.0, whose tasks have all completed, from pool
26/08/20 01:42:10 INFO DAGScheduler: ShuffleMapStage 43 (showString at NativeMethodAccessorImpl.java:0) finished in 0.117 s
26/08/20 01:42:10 INFO DAGScheduler: looking for newly runnable stages
26/08/20 01:42:10 INFO DAGScheduler: running: Set()
26/08/20 01:42:10 INFO DAGScheduler: waiting: Set()
26/08/20 01:42:10 INFO DAGScheduler: failed: Set()
26/08/20 01:42:10 INFO ShufflePartitionsUtil: For shuffle(14), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:42:10 INFO ShufflePartitionsUtil: For shuffle(14), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576
26/08/20 01:42:10 INFO HashAggregateExec: spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate.
26/08/20 01:42:10 INFO SparkContext: Starting job: showString at NativeMethodAccessorImpl.java:0
26/08/20 01:42:10 INFO DAGScheduler: Got job 27 (showString at NativeMethodAccessorImpl.java:0) with 1 output partitions
26/08/20 01:42:10 INFO DAGScheduler: Final stage: ResultStage 45 (showString at NativeMethodAccessorImpl.java:0)
26/08/20 01:42:10 INFO DAGScheduler: Parents of final stage: List(ShuffleMapStage 44)
26/08/20 01:42:10 INFO DAGScheduler: Missing parents: List()
26/08/20 01:42:10 INFO DAGScheduler: Submitting ResultStage 45 (MapPartitionsRDD[122] at showString at NativeMethodAccessorImpl.java:0), which has no missing parents
26/08/20 01:42:10 INFO MemoryStore: Block broadcast_30 stored as values in memory (estimated size 28.5 KiB, free 911.3 MiB)
26/08/20 01:42:10 INFO MemoryStore: Block broadcast_30_piece0 stored as bytes in memory (estimated size 13.2 KiB, free 911.3 MiB)
26/08/20 01:42:10 INFO BlockManagerInfo: Added broadcast_30_piece0 in memory on ip-10-0-1-54.ec2.internal:33771 (size: 13.2 KiB, free: 912.1 MiB)
26/08/20 01:42:10 INFO SparkContext: Created broadcast 30 from broadcast at DAGScheduler.scala:1592
26/08/20 01:42:10 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 45 (MapPartitionsRDD[122] at showString at NativeMethodAccessorImpl.java:0) (first 15 tasks are for partitions Vector(0))
26/08/20 01:42:10 INFO YarnScheduler: Adding task set 45.0 with 1 tasks resource profile 0
26/08/20 01:42:10 INFO TaskSetManager: Starting task 0.0 in stage 45.0 (TID 67) (ip-10-0-1-171.ec2.internal, executor 1, partition 0, NODE_LOCAL, 7374 bytes)
26/08/20 01:42:10 INFO BlockManagerInfo: Added broadcast_30_piece0 in memory on ip-10-0-1-171.ec2.internal:39119 (size: 13.2 KiB, free: 4.8 GiB)
26/08/20 01:42:10 INFO MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle 14 to 10.0.1.171:60082
26/08/20 01:42:10 INFO TaskSetManager: Finished task 0.0 in stage 45.0 (TID 67) in 22 ms on ip-10-0-1-171.ec2.internal (executor 1) (1/1)
26/08/20 01:42:10 INFO YarnScheduler: Removed TaskSet 45.0, whose tasks have all completed, from pool
26/08/20 01:42:10 INFO DAGScheduler: ResultStage 45 (showString at NativeMethodAccessorImpl.java:0) finished in 0.027 s
26/08/20 01:42:10 INFO DAGScheduler: Job 27 is finished. Cancelling potential speculative or zombie tasks for this job
26/08/20 01:42:10 INFO YarnScheduler: Killing all running tasks in stage 45: Stage finished
26/08/20 01:42:10 INFO DAGScheduler: Job 27 finished: showString at NativeMethodAccessorImpl.java:0, took 0.030510 s
+-------------+-----+
|tipo_vendedor|count|
+-------------+-----+
|entero       |69592|
|prefijado    |28056|
|mixto        |3852 |
+-------------+-----+
```

## 7. Regla de negocio para `total`

¿Qué regla de negocio permite detectar errores en `total`?

La regla de negocio es que total debe corresponder al producto de precio_unit × cantidad. Por tanto, se consideran errores o anomalías los registros donde total sea nulo o menor/igual a 0, y también aquellos donde total no coincida con precio_unit * cantidad.

## 8. Resumen para ti mismo

Antes de pasar a la Parte 2 (Bronze), resume en 3-4 líneas qué
decisiones de limpieza vas a tener que tomar en Silver a partir de lo
que encontraste aquí. No hace falta que sean las decisiones finales —
es tu plan de partida.

En Silver tendré que normalizar los valores categóricos, especialmente las variantes de region y canal, para evitar que diferencias de mayúsculas, espacios o nombres representen categorías distintas. También tendré que validar total mediante la regla precio_unit × cantidad y tratar los valores nulos, negativos o cero. Además, será necesario clasificar y normalizar vendedor_id según su formato y validar los correos electrónicos. Finalmente, tendré que decidir cómo tratar los registros con valores inválidos sin perder innecesariamente información útil.
