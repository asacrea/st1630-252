# Arquitectura — Lab 1a

**Curso:** ST1630-2026-2 · **Semana:** S4-S5 · **Fecha de entrega:** _(completar)_
**Estudiantes:** 

- Athina Alejandra Cappelletti García (aacappellg@eafit.edu.co).
- David Alejandro Gutiérrez Leal (dagutierrl@eafit.edu.co).
- Emmanuel Álvarez Castrillón (ealvarezc1@eafit.edu.co).
- Ginna Alejandra Valencia Macuace (gavalencim@eafit.edu.co).
- Mariamny Del Valle Ramírez Telles (mvramirezt@eafit.edu.co).
> Copia este archivo a tu carpeta de entrega (`entregas/<tu-usuario>/architecture.md`)
> y complétalo. No lo edites aquí en `plantillas/`.

## 1. Diagrama de la arquitectura

Pega tu diagrama (bloque \`\`\`mermaid o ASCII) mostrando: bucket S3 con las 3 capas, el rol IAM, y el clúster EMR leyendo desde S3.

> _(pega tu diagrama aquí)_

![Sistemas Intensivos Datos.drawio](https://hackmd.io/_uploads/rJIcXdcUMl.png)


## 2. Decisiones de S3

| Decisión | Tu elección | Justificación |
|---|---|---|
| Nombre del bucket |st1630-ealvarezc1-2026 |Corresponde a la convención definida para este laboratorio y permite identificar el bucket asociado al estudiante. La nomenclatura consistente facilita la administración, trazabilidad y organización de los recursos dentro del entorno de almacenamiento. |
| Región |us-east-1 |Porque se encuentra relativamente cerca de Colombia. Además, mantener los recursos de S3 y EMR en la misma región permite reducir la latencia de comunicación entre los servicios y evitar posibles costos de transferencia entre regiones. |
| Estructura de prefijos |bronze/, silver/, gold/ |En este laboratorio realizamos arquitectura por capas para separar los datos según su nivel de procesamiento, siguiendo el enfoque de medallion architecture. bronze conserva los datos originales, silver estaría destinada a datos limpiados/transformados y gold a datos procesados para consumo o análisis. |

**Justificación del particionamiento** (3-5 líneas): ¿por qué esa
estructura de prefijos y no otra? ¿Consideraste particionar además por
fecha o región dentro de cada capa?

Se utilizó una estructura por capas bronze/, silver/ y gold/ para separar los datos según su nivel de procesamiento, manteniendo los datos originales en Bronze y dejando Silver y Gold para futuras transformaciones. Dentro de Bronze se utilizó el prefijo ventas/ para organizar los datos por dominio. Para un volumen mayor de información, se podría extender la estructura con prefijos por región y fecha, por ejemplo bronze/ventas/Medellín/2026/08/ o bronze/ventas/Bogotá/2026/08/. Esto permitiría acceder directamente a las ventas de una región, de un día o mes específico, o combinar ambos criterios, reduciendo la cantidad de datos que Spark tendría que procesar.

La decisión de agregar particiones adicionales debería basarse en el volumen de datos y, principalmente, en los patrones de consulta esperados. No se recomienda agregar múltiples niveles de particionamiento únicamente con fines organizativos, ya que una granularidad excesiva puede generar una gran cantidad de archivos pequeños y aumentar la sobrecarga del procesamiento. Por esta razón, en este laboratorio se mantuvo una estructura sencilla y escalable, dejando la posibilidad de incorporar particiones adicionales cuando las necesidades del sistema lo justifiquen.

## 3. Decisiones de IAM

- ¿Qué permisos otorgaste al rol de EMR, exactamente?

 → No fue posible crear un rol IAM personalizado debido a que el entorno AWS Academy/Sandbox no contaba con permisos para ejecutar iam:CreateRole. Por esta razón, se utilizaron los roles de EMR que ya estaban disponibles en la cuenta:

**Service role:** (EMR_DefaultRole) fue utilizado por Amazon EMR para administrar el clúster. En nuestro laboratorio, EMR se encargó de crear y gestionar el clúster compuesto por 1 nodo master y 1 nodo core, donde posteriormente se ejecutó Spark. Para poder realizar estas operaciones, el servicio EMR necesitaba un rol que le permitiera interactuar con los recursos de AWS necesarios para crear y administrar el clúster.

**EC2 instance profile:** (EMR_EC2_DefaultRole) fue asociado a las instancias EC2 del clúster. Estas instancias son los nodos donde se ejecuta el procesamiento de Spark. El rol les proporciona los permisos necesarios para interactuar con otros servicios de AWS, especialmente S3, que fue donde almacenamos nuestro datalake. Gracias a estos permisos, los nodos pudieron acceder a los archivos que habíamos almacenado previamente en el bucket, como prueba_csv.csv y prueba_parquet.parquet.

EMR_DefaultRole tenía asociada la política administrada (AmazonElasticMapReduceRole), mientras que EMR_EC2_DefaultRole tenía asociada (AmazonElasticMapReduceforEC2Role). 

- ¿Qué permisos consideraste y descartaste? ¿Por qué?

 → Por las restricciones del entorno Sandbox no fue posible crear un rol propio mediante iam:CreateRole ni asignarle una política de mínimo privilegio que permitiera únicamente las operaciones necesarias sobre nuestro bucket S3, como GetObject, PutObject, DeleteObject y ListBucket.

Como alternativa, se utilizaron EMR_DefaultRole y EMR_EC2_DefaultRole, que ya estaban disponibles en la cuenta y contaban con las políticas administradas necesarias para ejecutar EMR. Al tratarse de roles estándar utilizados por EMR para funciones específicas, se decidió no modificar sus permisos, ya que hacerlo podía afectar el funcionamiento del servicio y no era necesario para completar el laboratorio. 

- ¿Por qué importa el mínimo privilegio específicamente en un sistema
  **distribuido** como este (no solo "es buena práctica")? Conecta con
  el Teorema CAP: un agente/rol con acceso excesivo es, en cierto
  sentido, un riesgo análogo al de un nodo que retorna datos
  inconsistentes — ambos rompen una garantía que el resto del sistema
  asume que se sostiene.

 → El mínimo privilegio es especialmente importante en un sistema distribuido porque un mismo rol puede ser utilizado por varios nodos/instancias del clúster. Si ese rol tiene permisos excesivos y una persona consigue acceder a él, podría robar, modificar o eliminar información a la que no debería tener acceso.

Esto se relaciona con el principio de única responsabilidad, ya que cada rol debería tener únicamente los permisos necesarios para cumplir su función. En nuestro caso, las instancias EC2 de EMR necesitan acceder a los datos del datalake en S3 para que Spark pueda procesarlos, pero no deberían tener acceso a otros recursos que no necesitan.

Por eso, el mínimo privilegio limita el daño que puede causar un rol si llega a ser utilizado indebidamente. Si el rol solo permite acceder a determinados datos y realizar determinadas operaciones, una persona que consiga utilizarlo tendrá un alcance mucho menor para robar, modificar o eliminar información.

## 4. Decisiones de EMR

- Tipo de instancia elegido y justificación (¿por qué es "mínimo
  viable" para este ejercicio, y qué cambiarías para producción?):

 → Se utilizaron instancias m5.xlarge, con un total de 2 nodos: 1 master y 1 core. Esta configuración se consideró mínima viable porque permite ejecutar Spark de forma distribuida, utilizando un nodo master para coordinar el procesamiento y un nodo core para ejecutar tareas.
 
Para un entorno de producción, podríamos aumentar el número de nodos y elegir los tipos de instancia de acuerdo con el tamaño de los datos y la carga de trabajo, buscando mayor capacidad de procesamiento, memoria y disponibilidad.

- Configuración de Spark/aplicaciones instaladas:

 → Se creó el clúster utilizando EMR 6.15.0 e instalando las aplicaciones Apache Spark y Hadoop. Spark fue utilizado como motor de procesamiento distribuido para leer y procesar los datos almacenados en nuestro datalake en S3, mientras que Hadoop forma parte del entorno de procesamiento proporcionado por EMR.
  
Además, mediante una bootstrap action se instalaron pandas y pyarrow en los nodos antes de iniciar Spark, para que estuvieran disponibles durante la ejecución del notebook de verificación.

## 5. Estimación de costo

Los siguientes cálculos los obtuvimos de la calculadora de AWS:
https://calculator.aws/#/createCalculator/ec2-enhancement

AWS Pricing Calculator presenta que el precio la instancia EC2 subyacente (m5.xlarge) corresponde a $0.192/hora:

AWS m5.xlarge Precio: 0.1920 USD/h (1 instancia)
Costo EC2 del clúster (2 Instancia): 2 × 0,192 = 0,384 USD/h 
Para 1 mes: 0,384 × 730h = 280,32 USD

| Escenario | Costo estimado |
|---|---|
| Clúster encendido 24/7 durante un mes | $280.32 USD |
| Clúster encendido solo durante las ~3 horas que lo usaste para el lab | $1.15 USD |


Sin embargo, Amazon EMR cobra un recargo adicional, aparte del EC2, por hora y por instancia. Este recargo cubre el servicio de administración del clúster: aprovisionamiento automático, instalación de Spark/Hadoop, y coordinación entre nodos. Se confirma en la página oficial de precios de EMR:
(https://aws.amazon.com/es/emr/pricing/)

Para resultados más exactos, considerando el recargo de EMR:
EC2 (por instancia): 0.192 USD/h
EMR (por instancia):0.048 USD/h

 0.192 USD/h + 0.048 USD/h = 0.240 USD/h

2 instancias (master + core):  0.240 × 2 = 0.480 USD/h (clúster completo)

**Mensual (24/7, 730h): 0.480 × 730 = 350.40 USD**
**Uso real (~3 horas):      0.480 × 3   = 1.44 USD**

## 6. Reflexión — la era agéntica

¿En qué decisión de este lab dudaste más? ¿Qué le consultaste a un
agente de IA y qué terminaste decidiendo por tu cuenta?

> La decisión que más nos generó dudas fue la configuración de los roles IAM, ya que el Sandbox no permitía crear un rol personalizado. Consultamos a ChatGPT para analizar el error, entender los permisos necesarios y revisar los roles existentes que podían servir. Finalmente, como equipo decidimos utilizar EMR_DefaultRole y EMR_EC2_DefaultRole, que ya contaban con las políticas necesarias para ejecutar EMR. La IA nos ayudó a investigar y comprender el problema, pero la decisión final fue nuestra.

## 7. Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
|Consulta de comando |Sí |Se utilizó IA como apoyo para identificar y comprender algunos comandos relacionados con la configuración y verificación de permisos. Los comandos fueron posteriormente ejecutados y comprobados en el entorno de AWS. |
|Explicación de conceptos |Sí |Durante el desarrollo del laboratorio se consultó al agente para comprender conceptos relacionados con IAM, EMR, S3, procesamiento distribuido y arquitectura de datos. |
|Verificación de tarifas de costo (EC2 + recargo EMR) |Sí | Se utilizó IA como apoyo para revisar los cálculos obtenidos mediante AWS Pricing Calculator y comprobar que la interpretación de los costos de EC2 y del recargo de EMR fuera coherente.|

> Recuerda: los permisos IAM, la estructura de prefijos, las
> justificaciones de este documento y la interpretación de los
> resultados de Spark deben reflejar tu propio criterio (ver
> `../../../docs/politica-ia.md`).
