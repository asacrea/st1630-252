# bitacora_delegacion.md — Lab 4a

**Estudiante:**
* Athina Alejandra Cappelleti García (aacappellg@eafit.edu.co)
* David Alejandro Gutiérrez Leal (dagutierrl@eafit.edu.co)
* Emmanuel Álvarez Castrillón (ealvarezc1@eafit.edu.co)
* Ginna Alejandra Valencia Macuace (gavalencim@eafit.edu.co)
* Mariamny Del Valle Ramírez Telles (mvramirezt@eafit.edu.co)

> Ver `../../../docs/politica-ia.md` para la política completa. Copia
> este archivo a `entregas/<tu-usuario>/bitacora_delegacion.md` y
> complétalo con tus propias filas — las de ejemplo de abajo son una
> guía de qué tan específica debe ser cada justificación, no algo para
> dejar tal cual.

**Agente usado:** Claude (chat). Lo usamos desde que apareció el primer error al levantar Docker (Parte 1) hasta la documentación (Parte 5). El agente ayudó en la redacción del borrador de `dag_design.md`. Nosotros ejecutamos el DAG, hicimos las pruebas y reunimos la evidencia (conteos, logs y capturas).

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Schedule interval elegido (`@daily`) | No | `@daily` ya venía definido en el código del lab, no fue una decisión nuestra ni una tarea que se le pidiera al agente. |
| Decisión de usar `catchup=False` | No | `catchup=False` ya venía definido en el código del lab, con el comentario que pide justificarlo en `dag_design.md`. No fue delegado. Lo que sí vimos en la ejecución fue que, al lanzar 7 fechas en paralelo, 3 fallaron en `transform_silver` con `ConcurrentAppendException`, y las repetimos luego una por una. |
| Estrategia de idempotencia en `ingest_bronze` (TODO 1) | Parcial | El lab ya traía la estrategia casi completa en el comentario del TODO: formato `parquet`, modo `overwrite` y la ruta `destino` de la partición `fecha={ds}`. El agente nos explicó por qué evita la duplicación (el `overwrite` solo reemplaza la partición de ese día). Después lo verificamos en la práctica: repetimos el backfill de las 7 fechas con `--reset-dagruns` y los conteos de Bronze no cambiaron (2957 antes y después). |
| Estrategia de idempotencia en `transform_silver` / MERGE (TODO 3) | No | El lab ya traía en el comentario del TODO el patrón del MERGE y la clave de negocio (`pedido_id`). Después comprobamos el resultado: Silver se mantuvo en 2820 filas con 2820 `pedido_id` distintos tras repetir las 7 fechas, y tras reprocesar 2026-01-08 con datos limpios subió a 3310 sin duplicados. Descubrimos además que el MERGE no protege de escrituras simultáneas (`ConcurrentAppendException`). |
| Diseño de los checks de `validate_bronze` (TODO 2) — qué cuenta como dato corrupto, qué umbral usar | Parcial | Las columnas críticas, el umbral y la descripción paso a paso de cada check ya venían en el archivo del lab (calcular la fracción de nulos por columna, contar `total <= 0` y lanzar `ValueError` con el detalle). Se escribió `check_nulos_criticos` y `check_rango_monto` siguiendo esas instrucciones. Nosotros provocamos el fallo de la Parte 4 con un script que nos dio el agente (vaciamos `total` en 41 de 510 filas de 2026-01-08, 8.04%) y verificamos que `validate_bronze` falló y que `transform_silver` y `build_gold` no se ejecutaron. Bronze subió a 3467 y Silver se quedó en 2820. |
| Threshold del check de nulos (5% — ¿tiene sentido para este dataset?) | No | `UMBRAL_NULOS = 0.05` ya venía definido en el código del lab y lo usamos tal cual. No hicimos un análisis propio de si es el valor adecuado para este dataset. Lo único que tenemos como evidencia es que las 7 fechas de enero pasaron `validate_bronze` con los datos originales, y que con 8.04% de nulos el check falló. Elegimos 8% para el fallo provocado para quedar por encima del umbral, no para evaluarlo. |
| Agregaciones de `build_gold` (TODO 4) | Parcial | El lab ya traía en el comentario del TODO el esqueleto de la primera agregación (`groupBy("region", "anio_mes")` con la suma y el conteo) y la descripción de la segunda (por `producto`, orden descendente y 20 filas). La task terminó en `success` en todas las corridas, pero no revisamos el contenido de las tablas de Gold por nuestra cuenta. |
| Redacción de `dag_design.md` | Sí | El agente ayudó en la redacción del documento a partir de la evidencia de nuestra propia ejecución (logs, comandos, conteos y el fallo provocado). Nosotros reunimos esa evidencia, revisamos el texto y completamos lo que faltaba. Las ideas de checks adicionales (duplicados de `pedido_id`, volumen esperado y consistencia `total = cantidad × precio_unit`) las propuso el agente. |
| Troubleshooting de Docker/Airflow (si tuviste algún problema de entorno) | Sí | Con ayuda del agente resolvimos: error de DNS de Docker al descargar la imagen, `pytest` ausente en el contenedor, el CSV no encontrado dentro del contenedor (`PATH_NOT_FOUND`), `Mkdirs failed` por permisos en la carpeta de salida y el `ConcurrentAppendException` del backfill paralelo. Nosotros ejecutamos los comandos de diagnóstico y los de corrección y leímos los logs que fueron necesarios. |