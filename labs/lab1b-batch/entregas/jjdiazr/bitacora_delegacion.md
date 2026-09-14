# Bitácora de delegación — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 2026-08-22
**Agente usado:** Claude Code (Opus 5), sesión interactiva

**Equipo:** Juan José Díaz Rodríguez · Juan Simón Ospina Martínez ·
Sebastián Durán Fernández · Daniel Arcila Salazar

**Declarante de esta bitácora:** Juan José Díaz Rodríguez —
jjdiazr@eafit.edu.co

> Esta bitácora declara **mis** delegaciones en la sesión de trabajo que
> produjo esta entrega. No habla por el resto del equipo: cada integrante
> declara las suyas.

> Conforme a `docs/politica-ia.md`. Declaro abajo toda delegación,
> incluidas las parciales.

## Tabla de delegación

| Tarea | ¿Delegado? | Detalle |
|---|---|---|
| Reconstrucción de la infraestructura del Lab 1a (bucket S3, clúster EMR) tras el borrado de AWS Academy | **Sí** | Troubleshooting de entorno, sin valor pedagógico. Incluyó corregir `create_emr.sh` (`--use-default-roles` es incompatible con `--ec2-attributes InstanceProfile`) |
| Generación del dataset y subida a S3 | **Sí** | Ejecución mecánica de `gen_dataset.py` y `aws s3 cp` |
| Ejecución de los scripts como steps de EMR y recuperación de logs desde S3 | **Sí** | Operación de infraestructura; alternativa a SSH al master |
| Diagnóstico de incompatibilidad de Delta Lake (`delta-spark:3.1.0` requiere Spark 3.5; el clúster corre 3.4.1) | **Sí** | Troubleshooting explícitamente delegable |
| Diagnóstico del registro fallido en Glue Catalog y solución vía DDL de Athena con `table_type='DELTA'` | **Sí** | Troubleshooting de configuración |
| Detección del desajuste entre el orden de columnas del README y el header real del CSV | **Sí** | El agente lo encontró al verificar `head -1` del CSV contra el TODO 1 de `01_bronze.py`; yo confirmé el impacto (mapeo posicional de `.schema()`) |
| Sintaxis de PySpark (`coalesce`, `regexp_extract`, `rlike`, `Window.rank`, API de `merge`) | **Sí** | Dudas puntuales de sintaxis, según la rúbrica |
| Código de los bloques `# TODO` de `01_bronze.py`, `02_silver.py`, `03_gold.py`, `04_athena_benchmark.py` | **Parcial** | El agente escribió el código; yo revisé cada bloque y validé los resultados contra mi profiling. Las decisiones de diseño marcadas abajo las tomé yo |
| **Contenido de `MAPA_REGION` y `MAPA_CANAL`** | **No** | Lo escribí yo a partir de mi propio profiling. El agente solo derivó mecánicamente la lista de claves distintas tras `upper(trim())` (18 de región, 17 de canal) y me explicó la trampa de las tildes; la asignación de cada clave a su valor canónico es mía |
| Decisión sobre `N/A`, `NA` y `Desconocido` → `OTRO` | **No** | Decisión propia, argumentada en `pipeline_analysis.md` (Pregunta 3) |
| Orden de `FORMATOS_FECHA` (`dd/MM/yyyy` antes de `MM/dd/yyyy`) | **No** | Decisión propia; el agente señaló la ambigüedad y cuantificó su costo (126 de 10.000 filas mal parseadas) |
| **Diseño del KPI 3** | **Parcial** | Yo elegí la métrica (`avg(calificacion)`), las dimensiones (`categoria` × `canal`) y la pregunta de negocio. El agente tradujo esa decisión a `groupBy/agg` y sugirió agregar `num_pedidos` y `tasa_devolucion` como métricas de apoyo |
| Interpretación del resultado del KPI 3 (hipótesis rechazada) | **No** | Conclusión propia sobre datos reales de mi ejecución |
| Columnas del `ZORDER BY` (`fecha, region`) | **Parcial** | El agente propuso el orden razonando desde el `WHERE` de la query 5.1; yo lo validé |
| **Clasificaciones NARROW/WIDE de las 15 operaciones** | **Parcial** | El criterio es mío: identifiqué que `dropDuplicates` es WIDE porque las filas iguales pueden estar en particiones distintas, que el hash va sobre la fila entera, que el `Window` necesita su propio shuffle porque `hash(categoria, producto) ≠ hash(categoria)`, que Bronze es NARROW porque ninguna fila mira a otra, y que el MERGE es WIDE porque hay que buscar el `pedido_id` en todas las particiones. El agente redactó esas justificaciones en prosa y las extendió a los casos análogos (los tres `groupBy` comparten el mismo razonamiento) |
| **Respuestas de `data_profiling.md` (8 preguntas)** | **Parcial** | Todos los datos y conclusiones son míos, leídos de mi propia ejecución: los conteos (1.500 duplicados, 8 variantes de Bogotá, 5 de app_movil, 3.959 filas con `total` inválido = 3,90%), que `vendedor_id` es string inconsistente, que son 5 formatos de fecha y no 4 porque `dd/MM` y `MM/dd` están mezclados en el patrón ambiguo, la regla `total = cantidad × precio_unit`, y la decisión de marcar los emails inválidos en vez de borrarlos. El agente organizó la evidencia bruta bajo cada pregunta y redactó mis respuestas en prosa |
| **Respuestas de `pipeline_analysis.md` (5 preguntas)** | **Parcial** | El razonamiento es mío, construido respondiendo preguntas del agente una por una: el mecanismo del shuffle (hash de la fila entera → misma partición → shuffle write a disco local → shuffle read por red), que descuentos/impuestos/envío romperían la validez de recalcular `total`, que un valor nuevo caería silenciosamente en `OTRO` y la solución de usar un centinela `NO_RECONOCIDO`, los cálculos de la Pregunta 4 (3.171 y 507 filas por partición; 128 vs. 800 shuffle files), y que el ratio del benchmark no es comparable porque una tabla está agregada y la otra no. El agente redactó esas respuestas en prosa y añadió detalle de contexto |
| Interpretación del benchmark Athena | **Parcial** | La observación central es mía (las dos tablas no miden lo mismo: 3.484 filas pre-agregadas contra 10.000 crudas). El agente la desarrolló y añadió que el efecto del Z-ordering no es aislable con este experimento |
| Redacción y formato de esta bitácora | **Parcial** | Redactada por el agente a partir del registro de la sesión; revisada y firmada por mí |

## Nota sobre el método de trabajo

Trabajé con el agente en modo "guiado por partes": en cada bloque me
explicaba qué hacía falta y por qué, escribía el andamiaje, y me dejaba
señalizados los puntos de decisión.

Para los documentos de análisis el método fue **preguntas y respuestas**.
El agente se negó dos veces a escribirme contenido que la rúbrica marca
como no delegable —primero `MAPA_REGION`/`MAPA_CANAL`, después las
respuestas de `pipeline_analysis.md`— citando la fila correspondiente. En
su lugar descomponía cada pregunta en preguntas cortas ("¿sobre qué
calcula Spark el hash: una columna o la fila entera?", "¿en un e-commerce
real, qué haría que el total no sea cantidad × precio?"), yo respondía, y
él transcribía mi respuesta al documento y la desarrollaba en prosa.

Declaro esa redacción como delegación parcial. La distinción que hago es
entre el **criterio**, que es mío y puedo defender, y la **redacción**,
que es del agente. `docs/politica-ia.md` lista "formatear o pulir la
redacción de un documento" entre lo delegable por defecto, pero prefiero
declararlo explícitamente antes que ampararme en esa categoría.

También corrigió un error conceptual mío: en mi primer intento dije que
los `Exchange` servían "para procesar más rápido", y me explicó que un
shuffle no es una optimización sino un costo que Spark está obligado a
pagar por corrección. Esa distinción no la tenía clara antes del lab.

## Declaración

Puedo explicar cada línea de los scripts entregados y cada número de los
documentos de análisis, y responder por qué tomé cada decisión.

El criterio es mío en todo lo que la rúbrica marca como no delegable: los
diccionarios `MAPA_REGION` y `MAPA_CANAL`, el tratamiento de `N/A` y
`Desconocido`, el orden de `FORMATOS_FECHA`, el diseño del KPI 3 y su
interpretación, las clasificaciones NARROW/WIDE y el razonamiento de las
respuestas de análisis.

La redacción en prosa de `data_profiling.md` y `pipeline_analysis.md` la
hizo el agente a partir de mis respuestas, y así queda declarado en la
tabla de arriba.

**Firma:** Juan José Díaz Rodríguez · jjdiazr@eafit.edu.co · 2026-08-21
