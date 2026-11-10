
# Proyecto Final SID

Emmanuel Cortes, Mariana Sanchez, Juan José Osorio y Sara Hurtado

Esta guía explica cómo ejecutar la ingesta de envíos y GPS en Kafka y cómo procesar esos eventos con Spark Structured Streaming. Los comandos son para PowerShell en Windows. Ejecútalos desde la carpeta `Proyecto_Final_SID`, donde está `docker-compose.yml`.

## 1. Preparar el equipo

Abre Docker Desktop y espera a que esté iniciado. Para ejecutar el productor local, necesitas Python 3.11 o superior. Crea el entorno una sola vez; si ya existe `.venv`, puedes saltar la primera línea:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Spark y Java se ejecutan dentro de Docker. La primera construcción necesita internet para descargar la imagen, las dependencias y los conectores de Spark.

## 2. Ejecutar Kafka y Spark

```powershell
docker compose --profile streaming up -d --build
docker compose --profile streaming ps -a
docker compose logs -f spark
```

`kafka` y `spark` deben quedar en ejecución. `spark-topics` crea los tópicos y termina con código `0`. Espera a que Spark termine de iniciar; el primer lote puede tardar más que los siguientes. Usa `Ctrl+C` para salir de los logs sin detener el servicio.

Si solo quieres probar la ingesta, inicia Kafka y crea los tópicos sin Spark:

```powershell
docker compose up -d --wait kafka
.\.venv\Scripts\python.exe -m ingestion topics
```

## 3. Enviar y leer eventos

Spark puede procesar los eventos que ya estén retenidos en Kafka. Si necesitas datos nuevos para una prueba, el simulador genera guías y posiciones GPS ficticias. El procesamiento de Spark usa los mensajes publicados; no fabrica alertas.

```powershell
.\.venv\Scripts\python.exe -m ingestion simulate --shipments 3 --steps 12 --scenario normal
.\.venv\Scripts\python.exe -m ingestion consume --limit 20
```

Una corrida de tres guías y doce pasos genera 36 eventos de envío y 36 posiciones GPS. El consumidor lee desde el inicio de los datos retenidos: las primeras 20 filas pueden ser de un solo tópico o de corridas anteriores. Para generar un caso con promesa vencida, usa `--scenario delay`. Para publicar continuamente, usa `--steps 0` y detén el simulador con `Ctrl+C`.

| Tópico | Contenido | Clave |
| --- | --- | --- |
| `shipment_events` | Cambios de estado de las guías | `shipment_id` |
| `vehicle_gps` | Coordenadas y velocidad de los vehículos | `vehicle_id` |
| `operational_updates` | Estado, riesgo y alertas calculados por Spark | `shipment_id` |
| `external_conditions` | Reservado para fuentes externas | Por definir |

## 4. Ver resultados de Spark

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic operational_updates --from-beginning --max-messages 5 --timeout-ms 30000
```

Busca `shipment_id`, `status`, `risk_level`, `active_alert` y `possible_delay_reason` en el JSON. `shipment_event_id` y `gps_event_id`, cuando está disponible, permiten rastrear los mensajes que dieron origen al resultado. Si el consumidor muestra `TimeoutException` después de imprimir mensajes, simplemente agotó el tiempo esperando más. Si no imprime ninguno, revisa los logs de Spark y comprueba que haya mensajes de entrada.

Spark guarda datos originales, eventos rechazados, eventos válidos y estados actuales en Delta Lake. Sus checkpoints quedan en `/data/streaming/checkpoints/main` dentro del volumen de Docker.

### Arquitectura medallón y modelo Gold

El job usa rutas explícitas por responsabilidad. Bronze conserva el mensaje Kafka sin modificar; Silver contiene registros validados, deduplicados y el estado enriquecido; Gold materializa el esquema estrella solicitado en el informe.

| Capa | Tabla Delta | Propósito |
| --- | --- | --- |
| Bronze | `bronze/events` | Payload original, topic, partición, offset y hora de ingreso para auditoría y reproceso. |
| Silver | `silver/events` | Eventos normalizados y deduplicados globalmente por `event_id`. |
| Silver | `silver/rejected_events` | Cuarentena con el error de validación y la referencia Kafka original. |
| Silver | `silver/current_shipments` | Último estado y evaluación operacional por guía. |
| Gold | `gold/fact_desempeno_envio` | Un registro por guía con tiempos real y prometido, retraso, permanencia conocida, movimientos, cumplimiento, riesgo y linaje. |
| Gold | `gold/dim_*` | Fecha, ruta, ciudad, centro logístico, vehículo y estado del envío. |
| Gold | `gold/indicadores_cumplimiento_diario` | Indicadores por fecha prometida y ruta para dashboards. |

Las claves dimensionales son determinísticas y los hechos se actualizan con `MERGE`, por lo que reintentar un microbatch no crea duplicados lógicos. El agregado de indicadores se reemplaza en una transacción Delta después de actualizar los hechos, evitando grupos obsoletos.

Para ejecutar las consultas auditables de cumplimiento (resumen general, cumplimiento por ruta, retrasos por destino, backlog de alto riesgo, permanencia en centros e indicadores diarios):

```powershell
docker compose --profile streaming run --rm --no-deps spark streaming/gold_queries.py --data /data/streaming --query all
```

También puedes elegir una consulta, por ejemplo `--query compliance_by_route`. Las sentencias revisables están en `queries/gold_compliance.sql`.

> Si el volumen fue creado por una versión anterior que guardaba tablas directamente bajo `/data/streaming`, usa un volumen nuevo para la validación de esta entrega o migra esas tablas antes de reutilizar el checkpoint. No borres un volumen con datos que necesites conservar.

## 5. Probar recuperación

**Reenvío de ingesta:** inicia el simulador con `--steps 0`. En otra terminal ejecuta `docker compose stop kafka`. Espera a que el simulador agote sus reintentos y se detenga. Consulta qué evento quedó pendiente:

```powershell
.\.venv\Scripts\python.exe -c "import sqlite3; c=sqlite3.connect('outbox.sqlite3'); print(c.execute('SELECT event_id,topic,key FROM pending').fetchall())"
docker compose up -d --wait kafka
.\.venv\Scripts\python.exe -m ingestion replay
```

La salida esperada es `Pendientes: 0`. Para comprobar que llegó a Kafka, busca el mismo `event_id` en el tópico que indicó SQLite. Por ejemplo, para un pendiente de GPS:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic vehicle_gps --from-beginning --timeout-ms 10000 | Select-String 'UUID_COPIADO'
```

**Recuperación de Spark:** ejecuta `docker compose restart spark` y revisa `docker compose logs --tail 50 spark`. El servicio debe retomar el procesamiento desde el checkpoint guardado. Para validar el avance, compara el número de lote antes y después del reinicio.

## 6. Ejecutar las pruebas y apagar

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
docker compose --profile streaming run --rm --no-deps --build spark tests/spark_integration.py
```

Las primeras pruebas se ejecutan sin Kafka ni Spark. La segunda usa Spark y Delta dentro de Docker con datos de prueba aislados; no publica en Kafka. Contrasta una entrega a tiempo, una entrega tardía y un envío abierto cuya promesa vence durante un tick técnico. Además, verifica la idempotencia del `MERGE`, las seis dimensiones, la tabla de hechos y los indicadores esperados (50 % de cumplimiento entre entregas finalizadas, sin contar envíos todavía abiertos en el denominador).

Para apagar los servicios conservando los datos y checkpoints:

```powershell
docker compose --profile streaming down
```

Para conservar los datos, ejecuta ese comando sin `-v`.

## 7. Configurar y ejecutar MongoDB

MongoDB es el componente encargado de almacenar el estado operacional más reciente de cada envío.

El consumidor desarrollado en `mongodb/consumer.py` recibe eventos desde el tópico Kafka `operational_updates` y actualiza la colección `shipments_current` de la base de datos `sid_logistica`.

### 7.1. Configurar las credenciales

Cada persona que clone el repositorio debe crear su propio archivo `.env`.

Desde la carpeta principal del proyecto, ejecuta:

```powershell
Copy-Item .env.example .env
```

Después abre `.env` y configura:

```dotenv
MONGO_USER=admin
MONGO_PASSWORD=TU_CONTRASENA_LOCAL
```

Reemplaza `TU_CONTRASENA_LOCAL` por una contraseña propia.

El archivo `.env` es privado y está excluido del repositorio mediante `.gitignore`. No debe subirse a GitHub.

**Importante:** MongoDB utiliza estas credenciales cuando inicializa su base de datos por primera vez. Si ya existe un volumen con usuarios creados anteriormente, modificar `.env` no cambia automáticamente sus contraseñas.

### 7.2. Iniciar MongoDB

Ejecuta:

```powershell
docker compose up -d --wait mongodb
```

Verifica su estado:

```powershell
docker compose ps mongodb
```

El contenedor debe aparecer como `healthy`.

MongoDB utiliza el puerto `27018` en el computador y el puerto `27017` dentro de Docker.

### 7.3. Cargar las credenciales en PowerShell

Para que los procesos Python puedan conectarse a MongoDB, es necesario cargar las variables del archivo `.env`.

En la terminal donde se ejecutará el consumidor o el dashboard, utiliza:

```powershell
Get-Content .env | Where-Object { $_ -match '^MONGO_(USER|PASSWORD)=' } | ForEach-Object {
    $nombre, $valor = $_ -split '=', 2
    [Environment]::SetEnvironmentVariable($nombre, $valor.Trim().Trim('"').Trim("'"), 'Process')
}
```

Después configura la URI de conexión:

```powershell
$env:MONGO_URI = "mongodb://$($env:MONGO_USER):$($env:MONGO_PASSWORD)@localhost:27018/?authSource=admin"
```

Si la contraseña contiene caracteres especiales reservados en una URI, estos deben codificarse adecuadamente.

Las variables se cargan únicamente en la terminal actual. Es necesario repetir este procedimiento al abrir una terminal nueva.

### 7.4. Ejecutar el consumidor de Kafka

Con Kafka y MongoDB funcionando, ejecuta:

```powershell
.\.venv\Scripts\python.exe -m mongodb.consumer
```

La salida esperada es:

```text
Conectado a MongoDB
Escuchando Kafka: operational_updates
```

Cuando lleguen nuevos eventos operacionales, se mostrarán mensajes como:

```text
Actualizado: GUIA-XXXX-0001
Actualizado: GUIA-XXXX-0002
```

El consumidor valida los mensajes recibidos y actualiza el documento correspondiente a cada `shipment_id`.

Esto permite mantener el último estado de cada guía sin crear un nuevo documento por cada cambio de estado.

Mantén abierta esta terminal mientras quieras recibir actualizaciones.

### 7.5. Consultar los datos almacenados

Para ingresar a MongoDB desde Docker:

```powershell
docker compose exec mongodb mongosh -u $env:MONGO_USER -p $env:MONGO_PASSWORD --authenticationDatabase admin
```

Dentro de MongoDB, selecciona la base de datos:

```javascript
use sid_logistica
```

Consulta todos los envíos:

```javascript
db.shipments_current.find().pretty()
```

Consulta la cantidad de envíos registrados:

```javascript
db.shipments_current.countDocuments({})
```

Consulta únicamente las alertas activas:

```javascript
db.shipments_current.find({ active_alert: true }).pretty()
```

Consulta un envío por su identificador:

```javascript
db.shipments_current.findOne({ shipment_id: "GUIA-XXXX-0001" })
```

Reemplaza el identificador de ejemplo por una guía generada durante la simulación.

Los documentos se conservan en el volumen persistente `mongo_data`.

## 8. Ejecutar el dashboard con Streamlit

El dashboard desarrollado en `dashboard/app.py` permite visualizar y consultar los estados operacionales almacenados en MongoDB.

### 8.1. Iniciar el dashboard

Abre una nueva terminal PowerShell dentro de la carpeta del proyecto.

Carga las credenciales de MongoDB siguiendo el procedimiento de la sección 7.3.

Después ejecuta:

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py
```

Streamlit mostrará una dirección local, normalmente:

```text
http://localhost:8501
```

Si el puerto está ocupado, podría utilizar otro puerto disponible, como `8502`. Abre la dirección indicada en la terminal.

### 8.2. Funcionalidades disponibles

El dashboard permite consultar:

- Cantidad total de envíos registrados.
- Cantidad de envíos entregados.
- Cantidad de envíos en tránsito.
- Cantidad de alertas activas.
- Distribución de envíos según su nivel de riesgo.
- Búsqueda de envíos por `shipment_id`.
- Tabla de estados operacionales almacenados en MongoDB.

Los indicadores se calculan a partir de la colección `shipments_current`.

El dashboard consulta el estado operacional en MongoDB. Las consultas analíticas de cumplimiento de la capa Gold se ejecutan por separado mediante Spark, según la sección 4.

### 8.3. Probar la actualización de datos

Con Spark, Kafka, MongoDB y el consumidor funcionando, abre una terminal adicional.

Ejecuta:

```powershell
.\.venv\Scripts\python.exe -m ingestion simulate --shipments 3 --steps 5 --interval 1 --scenario normal
```

El simulador generará eventos de envíos y GPS.

Spark procesará estos eventos y publicará los cambios operacionales en Kafka.

El consumidor actualizará MongoDB y el dashboard podrá consultar los nuevos estados.

Actualiza la visualización de Streamlit para comprobar los cambios.

Para detener el dashboard, utiliza `Ctrl+C` en la terminal donde se está ejecutando.

## 9. Ejecutar Apache Airflow

Apache Airflow se utiliza para supervisar el funcionamiento de los componentes del pipeline logístico.

El DAG desarrollado se encuentra en:

```text
airflow/dags/sid_pipeline.py
```

Su identificador es:

```text
sid_supervision
```

### 9.1. Iniciar Airflow

Primero asegúrate de tener configurado el archivo `.env` según la sección 7.1.

Inicia Kafka y MongoDB:

```powershell
docker compose up -d --wait kafka mongodb
```

Después inicia Apache Airflow:

```powershell
docker compose --profile orchestration up -d --build airflow
```

Verifica los contenedores:

```powershell
docker compose --profile orchestration ps
```

### 9.2. Acceder a la interfaz web

Abre:

```text
http://localhost:8088
```

Ingresa utilizando las credenciales configuradas por Airflow durante su inicialización.

En la interfaz, busca el DAG:

```text
sid_supervision
```

### 9.3. Tareas de supervisión

El DAG contiene tres tareas:

| Tarea | Función |
| --- | --- |
| `verificar_kafka` | Comprueba que el broker de Kafka sea accesible. |
| `verificar_mongodb` | Comprueba que MongoDB responda correctamente. |
| `supervisar_envios` | Consulta envíos registrados, alertas activas y envíos de alto riesgo. |

Las tareas `verificar_kafka` y `verificar_mongodb` se ejecutan primero.

Cuando ambas finalizan correctamente, se ejecuta `supervisar_envios`.

El DAG tiene una programación automática cada 10 minutos.

También puede ejecutarse manualmente desde la interfaz de Airflow.

### 9.4. Verificar las ejecuciones

Lista los DAG disponibles:

```powershell
docker compose exec airflow airflow dags list
```

Consulta el historial de ejecuciones:

```powershell
docker compose exec airflow airflow dags list-runs sid_supervision
```

El resultado esperado de una ejecución correcta es:

```text
state: success
```

Para revisar los registros:

```powershell
docker compose logs --tail 100 airflow
```

Una ejecución exitosa demuestra que las verificaciones y la consulta operacional se completaron correctamente.

### 9.5. Configuración de recursos

Para evitar una carga excesiva en equipos con memoria limitada, Airflow utiliza un paralelismo reducido.

Verifica la configuración:

```powershell
docker compose exec airflow airflow config get-value core parallelism
```

Resultado esperado:

```text
2
```

Esta configuración limita la cantidad de tareas que Airflow puede ejecutar simultáneamente.

### 9.6. Detener Airflow

```powershell
docker compose stop airflow
```

Airflow conserva su información local en el volumen Docker `airflow_data`.

**Nota:** el DAG actual supervisa Kafka, MongoDB y los estados operacionales. No inicia automáticamente el simulador, Spark ni el consumidor de MongoDB.

## 10. Ejecutar el proyecto completo

Esta sección reúne los pasos necesarios para comprobar el flujo completo de extremo a extremo:

```text
Simulador Python
       |
       v
Kafka (shipment_events / vehicle_gps)
       |
       v
Spark Structured Streaming
       |
       +----> Delta Lake (Bronze / Silver / Gold)
       |
       v
Kafka (operational_updates)
       |
       v
Consumidor Python
       |
       v
MongoDB
       |
       v
Streamlit

Airflow: supervisión programada de Kafka y MongoDB
```

### 10.1. Preparar el entorno

Instala las dependencias de Python:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Si el entorno virtual ya existe, no es necesario crearlo nuevamente.

Crea el archivo `.env` si todavía no existe:

```powershell
Copy-Item .env.example .env
```

Configura las credenciales locales de MongoDB siguiendo la sección 7.1.

### 10.2. Terminal 1: iniciar Kafka, MongoDB y Spark

```powershell
docker compose --profile streaming up -d --build
```

Comprueba el estado:

```powershell
docker compose --profile streaming ps -a
```

Los contenedores de Kafka, MongoDB y Spark deben estar en ejecución.

El servicio `spark-topics` puede aparecer como finalizado correctamente, ya que su función es crear los tópicos de Kafka.

La primera ejecución de Spark puede tardar debido a la descarga de dependencias y la inicialización de los conectores.

### 10.3. Terminal 2: ejecutar el consumidor MongoDB

Abre una nueva terminal y carga las variables de entorno:

```powershell
Get-Content .env | Where-Object { $_ -match '^MONGO_(USER|PASSWORD)=' } | ForEach-Object {
    $nombre, $valor = $_ -split '=', 2
    [Environment]::SetEnvironmentVariable($nombre, $valor.Trim().Trim('"').Trim("'"), 'Process')
}

$env:MONGO_URI = "mongodb://$($env:MONGO_USER):$($env:MONGO_PASSWORD)@localhost:27018/?authSource=admin"
```

Inicia el consumidor:

```powershell
.\.venv\Scripts\python.exe -m mongodb.consumer
```

Debe mostrar:

```text
Conectado a MongoDB
Escuchando Kafka: operational_updates
```

Mantén esta terminal abierta.

### 10.4. Terminal 3: ejecutar Streamlit

Abre otra terminal.

Carga nuevamente las variables de MongoDB, como se indica en la sección 10.3.

Ejecuta:

```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py
```

Abre la dirección indicada, normalmente:

```text
http://localhost:8501
```

### 10.5. Terminal 4: generar los eventos

Ejecuta:

```powershell
.\.venv\Scripts\python.exe -m ingestion simulate --shipments 3 --steps 5 --interval 1 --scenario normal
```

El resultado esperado al terminar es:

```text
Pendientes: 0
```

Esto indica que no quedaron eventos pendientes en el outbox del simulador.

### 10.6. Comprobar la salida de Spark

Ejecuta:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic operational_updates --from-beginning --timeout-ms 10000
```

Busca los envíos generados por el simulador.

Los mensajes deben contener información como:

```json
{
  "schema_version": "1.0",
  "event_type": "operational_update",
  "shipment_id": "GUIA-XXXX-0001",
  "status": "delivered",
  "active_alert": false,
  "risk_level": "low"
}
```

Este JSON es un ejemplo simplificado de los campos relevantes. Los identificadores y valores reales dependen de los eventos procesados.

Si aparecen mensajes nuevos, Spark habrá publicado las actualizaciones operacionales.

### 10.7. Comprobar MongoDB y Streamlit

En la terminal del consumidor deben aparecer mensajes como:

```text
Actualizado: GUIA-XXXX-0001
Actualizado: GUIA-XXXX-0002
```

Después abre Streamlit y comprueba que los indicadores reflejen los estados almacenados en MongoDB.

Puedes buscar una guía generada durante la simulación para verificar que su información esté disponible.

### 10.8. Ejecutar la supervisión con Airflow

Con Kafka y MongoDB funcionando, inicia Airflow:

```powershell
docker compose --profile orchestration up -d airflow
```

Accede a:

```text
http://localhost:8088
```

Busca `sid_supervision` y comprueba el estado de sus ejecuciones.

También puedes consultar el historial desde PowerShell:

```powershell
docker compose exec airflow airflow dags list-runs sid_supervision
```

### 10.9. Recomendaciones para equipos con recursos limitados

Spark y Airflow pueden consumir una cantidad considerable de memoria y CPU.

Si el equipo presenta lentitud, puede ejecutarse primero la prueba de streaming y posteriormente la prueba de supervisión.

Para detener Spark sin eliminar sus datos:

```powershell
docker compose stop spark
```

Después inicia Airflow:

```powershell
docker compose --profile orchestration up -d airflow
```

Kafka y MongoDB deben permanecer funcionando para que Airflow pueda comprobar sus conexiones.

### 10.10. Apagar todo el proyecto

Detén primero los procesos locales del consumidor MongoDB y Streamlit utilizando `Ctrl+C` en sus terminales.

Después ejecuta:

```powershell
docker compose --profile streaming --profile orchestration down
```

Este comando detiene y elimina los contenedores del proyecto, pero conserva los volúmenes persistentes de Docker.

Para conservar los datos de Kafka, MongoDB, Delta Lake y Airflow, no agregues la opción `-v`.

Cuando vuelvas a iniciar el proyecto, los datos almacenados en los volúmenes seguirán disponibles.
