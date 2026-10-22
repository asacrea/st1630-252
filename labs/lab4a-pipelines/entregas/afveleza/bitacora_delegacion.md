# bitacora_delegacion.md — Lab 4a

**Equipo:**
- Andrés Felipe Vélez Álvarez (`afveleza@eafit.edu.co`)
- Samuel Samper Cardona (`ssamperc@eafit.edu.co`)
- Sebastián Salazar Henao (`salazarh3@eafit.edu.co`)
- Hellen Yanes Doria (`hyanesd@eafit.edu.co`)

**Forma de trabajo:** el lab se desarrolló como trabajo conjunto entre el
equipo y un agente de IA (Claude). El equipo guió el proceso paso a paso:
decidió el orden de trabajo, planteó las ideas y criterios de cada decisión,
ejecutó todos los comandos, corrió el DAG, tomó las capturas y verificó los
resultados. El agente explicó los conceptos, propuso implementaciones y
mejoras sobre las ideas del equipo, revisó el código en cada paso y apoyó la
redacción.

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Schedule interval elegido (`@daily`) | Parcial | Idea del equipo: el área de analítica revisa estas métricas a diario; cada hora es demasiado frecuente y cada semana demasiado tarde. El agente la complementó con argumentos técnicos (granularidad diaria del dato, costo de levantar Spark por corrida) y apoyó la redacción. |
| Decisión de usar `catchup=False` | Parcial | Idea del equipo: al activar el DAG no se deben re-ejecutar automáticamente las fechas pasadas. Con el agente se precisó el mecanismo (Airflow crearía una corrida por cada intervalo pendiente desde `start_date`, ~287 en este caso) y se añadió el trade-off de que los huecos se rellenan con backfill explícito. Redacción apoyada por el agente. |
| `max_active_runs=1` (decisión adicional) | Parcial | Surgió al analizar en conjunto el riesgo de concurrencia del `if isDeltaTable`. Criterio del equipo: evitar que dos corridas escriban los mismos archivos a la vez y dañen los datos. El agente detalló los tres riesgos concretos (primera escritura, MERGE simultáneos, memoria) y apoyó la redacción. |
| Estrategia de idempotencia en `ingest_bronze` (TODO 1) | Parcial | La estrategia (`overwrite` solo sobre la carpeta `fecha={ds}`, no `append`) venía indicada en la pista del TODO. El agente escribió la línea siguiendo esa pista y explicó append vs. overwrite; el equipo la integró, explicó por qué no afecta los demás días y la verificó con los conteos de los dos backfills y con la recuperación del 2026-01-08. |
| Estrategia de idempotencia en `transform_silver` / MERGE (TODO 3) | Parcial | El patrón del MERGE por `pedido_id` venía escrito en el comentario del TODO; el código se escribió en conjunto siguiéndolo (update si existe, insert si no). El equipo planteó por qué un `append` duplicaría al reintentar; el agente lo amplió con el escenario de reintento tras un MERGE ya ejecutado. Verificado por el equipo con el historial Delta (v6 → v13 sin crecer filas). |
| Transformaciones de `transform_silver` (casts, filtro de devueltos, `anio_mes`) | Parcial | Código en conjunto: el agente propuso la implementación y explicó cada paso; el equipo la integró y validó el resultado (2,957 filas Bronze → 2,820 Silver, diferencia = pedidos devueltos). |
| Diseño de los checks de `validate_bronze` (TODO 2) — qué cuenta como dato corrupto | Parcial | Las reglas (umbral 5%, `total <= 0`) venían del enunciado. Código en conjunto con el agente, validado por el equipo con pytest (3/3) y con el fallo provocado de la Parte 4. Los criterios de por qué un check tiene umbral, otro tolerancia cero y otro solo avisa fueron del equipo; el agente los enriqueció (efecto de un `pedido_id` nulo sobre el MERGE). Los 3 checks adicionales para producción que aparecen en `dag_design.md` (volumen, duplicados, consistencia del monto) los propuso el agente. |
| Threshold del check de nulos (5%) | Parcial | Criterio del equipo: es normal que en datos reales existan algunos nulos, pero un porcentaje alto indica un problema en la fuente. El agente añadió el riesgo específico de nulos en `total` y `pedido_id` y la recomendación de calibrar el umbral con el histórico. |
| Agregaciones de `build_gold` (TODO 4) | Parcial | Código en conjunto siguiendo la pista del TODO; el equipo verificó que la suma de `n_transacciones` en Gold (2,820) cuadra con Silver. |
| Redacción de `dag_design.md` | Parcial | Las ideas y criterios de schedule, idempotencia y checks los planteó el equipo; el agente los mejoró y redactó. Las secciones de backfill/`ds`, alertas y reflexión se construyeron con el agente a partir de lo trabajado en conjunto durante el lab y fueron revisadas por el equipo. Todos los números, logs y capturas provienen de la ejecución del equipo. |
| Troubleshooting de Docker/Airflow | Sí | Realizado por el agente: detectó que el CSV debía generarse dentro de `datos/`, revisó la RAM de Docker en WSL2, la descarga lenta de pyspark, la ausencia de `pytest` (usar `python -m pip/pytest`) y el error `Mkdirs failed` por permisos del volumen (`chown` a uid 50000). El equipo ejecutó las soluciones. |
| Scripts de evidencia (`scripts/contar_filas.py`, `scripts/corromper_fecha.py`) | Sí | Escritos por el agente para obtener los conteos de Bronze/Silver/Gold y corromper el CSV de forma reproducible. Después los amplió: `corromper_fecha.py` con un modo `negativo` para probar `check_rango_monto`, y `contar_filas.py` con `top_productos` y el historial Delta de Gold. El equipo los ejecutó y usó sus resultados como evidencia. |
| Revisión de la entrega antes de la versión final | Parcial | El agente revisó la primera versión del PR contra la rúbrica y señaló lo que faltaba: probar el segundo check duro con datos reales, la evidencia de pytest y de `top_productos`, y tiempos de ejecución que no cuadraban con las capturas. También señaló que los reintentos no sirven ante un error de datos. El equipo corrió el fallo 2 (2026-01-09), la recuperación del 2026-01-08, pytest y los conteos finales; el agente redactó esas secciones de `dag_design.md` con los números del equipo. |
