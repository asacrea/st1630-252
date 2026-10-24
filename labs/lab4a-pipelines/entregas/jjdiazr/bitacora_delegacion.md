# bitacora_delegacion.md — Lab 4a

**Estudiante:** Juan José Díaz Rodríguez — jjdiazr@eafit.edu.co  

---

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Schedule interval elegido (`@daily`) | No | Decisión propia basada en el caso de negocio: Nequi cierra operaciones diariamente y el equipo de analítica requiere el tablero consolidado a primera hora de la mañana. |
| Decisión de usar `catchup=False` | No | Decisión propia basada en la arquitectura: habilitar `catchup=True` tras períodos de inactividad dispararía decenas de corridas simultáneas saturando la memoria RAM local y bloqueando PostgreSQL. |
| Estrategia de idempotencia en `ingest_bronze` (TODO 1) | No | Se diseñó la partición diaria con modo `overwrite` acotado a la carpeta `fecha=ds` para que cualquier reejecución reemplace únicamente ese día sin tocar otras fechas ni acumular duplicados. |
| Estrategia de idempotencia en `transform_silver` / MERGE (TODO 3) | No | Se definió el uso de `DeltaTable.merge()` utilizando `pedido_id` como clave única, garantizando que reejecuciones actualicen (`whenMatchedUpdateAll`) en lugar de insertar copias redundantes (`whenNotMatchedInsertAll`). |
| Diseño de los checks de `validate_bronze` (TODO 2) | No | Se definió que los nulos en campos clave (`pedido_id`, `producto`, `total`) y los montos $\le 0$ son fallos duros que invalidan la transacción, mientras que la cardinalidad de canal solo amerita un warning ya que puede obedecer a aperturas comerciales legítimas. |
| Threshold del check de nulos (5% — ¿tiene sentido para este dataset?) | No | Análisis propio: un margen del 5% permite tolerar anomalías esporádicas de transmisión sin tumbar el pipeline completo, pero bloquea desviaciones severas (como la prueba provocada con 7.84% de nulos). |
| Agregaciones de `build_gold` (TODO 4) | Parcial | Definí las agrupaciones de negocio (`region, anio_mes` y `producto` ordenado desc con top 20); utilicé el agente para validar la sintaxis puntual de PySpark (`groupBy`, `agg`, `F.sum`, `F.count`). |
| Redacción de `dag_design.md` | Parcial | Los argumentos, decisiones de diseño, métricas y logs reales fueron obtenidos de mi ejecución; delegué al asistente la estructuración y formateo de la plantilla markdown. |
| Troubleshooting de Docker/Airflow | Parcial | El agente asistió en diagnosticar la falta de la librería `pytest` dentro del contenedor y el ajuste de permisos de escritura (`chmod 777` / `chown airflow`) en el volumen montado de salida. |
