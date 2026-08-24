
**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 23/08/2026
**Estudiante:** _Nathalia Cardoza (`nvcardozaa@eafit.edu.co`), Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango_

Resolucion preguntas de 01_bronze.py
- "commitInfo"  -> ¿quién hizo el commit? R. Esto se evidencia en la parte que dice "engineInfo" en la que aparece "Apache-Spark/4.2.0 Delta-Lake/4.4.0"; ¿cuándo? R. el timestamp está en milisegundos desde época Unix (1787472593927) — corresponde a las 03:09:52 (hora local), coincide con _ingested_at que se ve más abajo en los add's; ¿con qué operación? R. WRITE en modo Append, como se definió en el TODO 4.
- "metaData"  -> ¿coincide el schema con tu BRONZE_SCHEMA + las 2
columnas de auditoría del TODO 3? R. Si, ya que lista las 14 columnas otiginales mas las 2 de auditoría ("_ingested_at","type":"timestamp" y "_source_file","type":"string") al final.
- "add"  -> ¿cuántos archivos Parquet agregó este commit? R. 4 archivos () que coinciden con el numFiles: 4 de commitInfo 


Troubleshooting Spark/Delta: Al ejecutar 01_bronze.py localmente, la verificación automática falló porque el orden real de columnas en ventas_colombia_raw.csv no coincidía con el orden asumido en el comentario original de BRONZE_SCHEMA (region/canal aparecían casi al final del CSV, no en la posición 3-4 como ponía el comentario). Al aplicar un schema explícito, Spark asigna columnas por posición, no por nombre del header, así que el cambio causó que los datos quedaran mal alineados (ej. total mostraba 0 nulos en vez de los aprox. 2,571 esperados). Consulté con un agente de IA para diagnosticar el error y el diagnóstico fue de configuración/mapeo de columnas, no una decisión de diseño del pipeline. Corregí el orden de BRONZE_SCHEMA para que coincidiera con el header real del CSV y se logró solucionar el problema.


Troubleshooting Spark/Delta (fechas): Al ejecutar 02_silver.py localmente con F.to_date(), Spark lanzó CANNOT_PARSE_TIMESTAMP en vez de devolver null cuando un formato de fecha no coincidía con el valor real (ej. probar dd/MM/yyyy contra una fecha con guiones). Esto ocurrió porque mi Spark local (4.2.0) tiene el modo ANSI activado por defecto, a diferencia del Spark 3.x del clúster EMR del curso, donde to_date() sí devuelve null silenciosamente en un parseo fallido. Consulté con un agente de IA, que identificó la causa como una diferencia de configuración entre versiones de Spark, no un error de lógica del coalesce(). La solución (sugerida por el mismo mensaje de error de Spark) fue reemplazar F.to_date() por F.try_to_date(), que tolera formatos inválidos devolviendo null independientemente del modo ANSI, resolviendo asi el error presentado.


Troubleshooting Spark/Delta (rutas en OPTIMIZE): spark.sql("OPTIMIZE delta.\...") interpreta la ruta dentro del parser SQL de Spark, no como un path de Python — con rutas relativas (../datos/...) esto puede no resolver igual que spark.read.load(). Convertí la ruta a absoluta con os.path.abspath() antes de pasarla alOPTIMIZE` para evitar la ambigüedad y que asi no me saltara el mismo error de la ejecucion que no tenia este cambio implementado.


Troubleshooting infraestructura (permisos y red del sandbox):
- setup_iam.sh falló con AccessDenied en iam:CreateRole — la plataforma sandbox no permite crear roles IAM nuevos. Usé el rol EMR_EC2_DefaultRole ya provisto por la plataforma en su lugar, y --service-role EMR_DefaultRole en vez de --use-default-roles para evitar el conflicto de flags de la CLI.
- El primer clúster EMR falló (TERMINATED_WITH_ERRORS, VALIDATION_ERROR) porque la única tabla de rutas de la VPC por defecto no tenía ruta hacia el Internet Gateway, aunque el IGW sí existía y estaba attachado. Agregué la ruta faltante manualmente con aws ec2 create-route (sí tenía permiso ec2:CreateRoute), y el segundo intento de creación del clúster funcionó.


Nota metodológica — Spark UI vs. plan físico: Trabajé contra el clúster EMR vía SSH (sin acceso a EMR Studio/interfaz gráfica), así que no pude capturar el DAG visual de Spark UI como sugiere la Parte 3.8 del README. En su lugar, usé la salida de .explain(mode="formatted") que ya genera 02_silver.py; es la misma información (el plan físico de ejecución) en formato de texto en vez de visual, y permite identificar los nodos Exchange de la misma manera.