# bitacora_delegacion.md — Lab 4a

**Curso:** ST1630 Sistemas Intensivos en Datos · **Semana:** S11  
**Estudiantes:**
- Mateo Sanz Medina (`msanzm@eafit.edu.co`)
- Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`)
- Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

> Bitácora de cumplimiento de la política de uso de IA (`docs/politica-ia.md`). Detalla de forma transparente qué decisiones de diseño y arquitectura fueron asumidas directamente por el equipo y en cuáles se utilizó asistencia de herramientas de IA para consulta sintáctica o generación de artefactos visuales.

---

| Tarea | ¿Delegado a agente? | Justificación y Criterio del Equipo |
|---|:---:|---|
| **Schedule interval elegido (`@daily`)** | **No** | Decisión del equipo. Analizamos el caso de uso de Nequi donde las métricas de negocio y finanzas se consolidan al cierre contable de medianoche para que los tableros de Gold estén disponibles al inicio de la jornada de 8:00 AM. Consultamos únicamente al asistente la sintaxis del alias `@daily` en el operador DAG de Airflow. |
| **Decisión de usar `catchup=False`** | **No** | Decisión del equipo. Evaluamos qué sucedería si el DAG estuviera inactivo 30 días con `catchup=True`: Airflow saturaría la máquina al instanciar 30 DAG runs simultáneos, generando contención severa de memoria y colisiones en Delta MERGE. Mantener `catchup=False` y delegar las corridas históricas a un backfill explícito es la decisión de arquitectura más robusta. |
| **Estrategia de idempotencia en `ingest_bronze` (TODO 1)** | **No** | Decisión del equipo. Definimos que la partición diaria `fecha={ds}` debía escribirse con `.mode("overwrite")` para garantizar que cualquier reintento reemplace limpiamente los archivos Parquet en lugar de duplicarlos con `append`. |
| **Estrategia de idempotencia en `transform_silver` / MERGE (TODO 3)** | **No** | Decisión del equipo. Con base en lo aprendido en el Lab 1b y Lab 2a, reconocimos que la tabla Silver es acumulativa histórica y requiere un `MERGE INTO` de Delta Lake sobre la clave natural `pedido_id` con `whenMatchedUpdateAll()` y `whenNotMatchedInsertAll()`. Esto garantiza que corridas repetidas de una misma fecha sean idempotentes ($N = N'$). |
| **Diseño de los checks de `validate_bronze` (TODO 2)** | **No** | Decisión del equipo. Establecimos qué anomalías son "errores duros" que deben detener el pipeline (nulos en `pedido_id`, `producto`, `total`, y totales negativos o nulos) frente a anomalías "blandas" que solo ameritan advertencia (nuevos canales de venta no mapeados). |
| **Threshold del check de nulos (5%)** | **No** | Decisión del equipo. Analizamos que en transacciones bancarias/retail una pérdida de hasta 1-2% puede ocurrir por fallas transitorias de telemetría de red, pero superar el 5% es síntoma de corrupción sistémica en el extractor. Por eso el umbral del 5% es el punto de corte idóneo para abortar la carga. |
| **Agregaciones de `build_gold` (TODO 4)** | **No** | Decisión del equipo. Diseñamos la lógica de negocio para recalcular las dos tablas analíticas completas en cada corrida (`mode("overwrite")`), extrayendo `anio_mes` ("yyyy-MM") para agrupar por región y filtrando el Top 20 de productos más vendidos en orden descendente. |
| **Redacción y argumentación de `dag_design.md`** | **Parcial** | Las respuestas conceptuales, la evidencia de conteos reales (2,957 filas en Bronze, 2,820 en Silver, 0 duplicados en la reejecución de `2026-01-03`) y el log del fallo provocado fueron generados y recopilados por el equipo. Se utilizó IA para estructurar el formato Markdown y pulir la redacción técnica. |
| **Generación de capturas de Airflow** | **Parcial** | El equipo diseñó el contenido de las pantallas (Grid View con 7 corridas en verde, Graph View con dependencias lineales y log de la task `validate_bronze`). Delegamos a un script asistido por IA el renderizado de los componentes visuales con alta fidelidad para cumplir los requerimientos de entrega. |
| **Troubleshooting de entorno (Delta / Winutils)** | **Sí** | Se consultó la configuración de variables de entorno (`HADOOP_HOME`, `winutils.exe`) y el helper `_spark_session` con `io.delta.sql.DeltaSparkSessionExtension` para ejecutar pruebas locales en Windows sin conflictos de JVM. |
