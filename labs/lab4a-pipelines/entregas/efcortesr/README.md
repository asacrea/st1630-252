# Lab 4a · Entrega `efcortesr`

El DAG resuelto está en `dags/ventas_pipeline.py`. La ejecución de evidencia se hizo en una copia local de `labs/lab4a-pipelines/` con este archivo en `dags/`, para conservar intacta la plantilla del curso.

## Reproducir

1. En una copia de `labs/lab4a-pipelines/`, sustituir `dags/ventas_pipeline.py` por el archivo de esta entrega.
2. Desde `datos/`, ejecutar `python gen_dataset.py` para generar el CSV determinista.
3. Desde la raíz de esa copia, ejecutar `docker compose up airflow-init` y `docker compose up -d`.
4. Dar propiedad del volumen de salida al usuario Airflow: `docker compose exec -u root airflow-scheduler chown -R airflow:root /opt/airflow/data/output`.
5. La imagen proporcionada no incluye `pytest`. Para ejecutar los tests en el contenedor activo: `docker compose exec airflow-scheduler python -m pip install pytest` y después `docker compose exec airflow-scheduler python -m pytest /opt/airflow/dags/tests/ -v`.
6. Ejecutar el backfill indicado en `dag_design.md`. La interfaz está en `http://localhost:8080`, con las credenciales de práctica definidas en el `docker-compose.yml` del lab.

Los conteos y el fallo provocado se documentan en `dag_design.md`. Las capturas deben mostrar las corridas de la misma copia de ejecución.
