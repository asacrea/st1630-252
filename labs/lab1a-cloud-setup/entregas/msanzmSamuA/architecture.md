# Arquitectura — Lab 1a

**Curso:** ST1630-2026-2 · **Semana:** S4-S5 · **Fecha de entrega:** 2026-08-13  
**Equipo:** Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango, Nathalia Cardoza

---

## 1. Diagrama de la arquitectura

```
                    Cuenta AWS Academy (us-east-1)
   ┌─────────────────────────────────────────────────────────────┐
   │                                                             │
   │   S3 Data Lake: s3://st1630-msanzm-2026                     │
   │   ┌───────────────┐   ┌───────────────┐   ┌───────────────┐ │
   │   │    bronze/    │   │    silver/    │   │     gold/     │ │
   │   │ (datos crudos)│   │ (datos clean) │   │ (agregados)   │ │
   │   └───────────────┘   └───────────────┘   └───────────────┘ │
   │           ▲                                                 │
   │           │ Lectura/Escritura estricta                      │
   │           │ (GetObject, PutObject, ListBucket)              │
   │           │                                                 │
   │   ┌───────┴──────────────────────────────┐                  │
   │   │  Rol IAM / Instance Profile          │                  │
   │   │  LabInstanceProfile / EMR_EC2_role   │                  │
   │   │  Recurso acotado a tu propio bucket  │                  │
   │   └───────┬────────────────────────────┬─┘                  │
   │           │ Asumido por                │                    │
   │   ┌───────┴──────┐              ┌──────┴──────┐             │
   │   │  EMR Master  │◄────────────►│  EMR Core   │ Clúster EMR │
   │   │  (m4.large)  │   PySpark    │ (m4.large)  │ (emr-6.15)  │
   │   └──────────────┘              └─────────────┘             │
   │                                                             │
   └─────────────────────────────────────────────────────────────┘
```

---

## 2. Decisiones de S3

| Decisión | Tu elección | Justificación |
|---|---|---|
| Nombre del bucket | `st1630-msanzm-2026` | Sigue la convención del curso (`st1630-{usuario}-{año}`), garantizando unicidad global en S3. |
| Región | `us-east-1` | Coincide con la región asignada por el entorno AWS Academy / Learner Lab, minimizando latencias de API. |
| Estructura de prefijos | `bronze/`, `silver/`, `gold/` | Implementación directa de la Arquitectura Medallion para separar datos crudos, limpios y analíticos. |

**Justificación del particionamiento:**  
Se definió la estructura de capas Medallion (`bronze/ventas/`) para aislar los datos crudos en su formato original de las transformaciones posteriores. Para este dataset de prueba (10.000 filas), particionar adicionalmente por fecha o región generaría el problema del *small files problem* (archivos Parquet sub-óptimos de pocos kilobytes). En un entorno productivo de mayor volumen (escala Terabytes), se particionaría por fecha (`year=/month=/day=`) en `silver/` y `gold/` para aprovechar el *partition pruning* de Spark.

---

## 3. Decisiones de IAM

- **¿Qué permisos otorgaste al rol de EMR, exactamente?**  
  Se otorgaron los permisos estrictamente necesarios para la lectura, escritura y listado de objetos: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` sobre `arn:aws:s3:::st1630-msanzm-2026/*` y `s3:ListBucket` sobre `arn:aws:s3:::st1630-msanzm-2026`.

- **¿Qué permisos consideraste y descartaste? ¿Por qué?**  
  Se descartó rotundamente utilizar políticas permisivas como `s3:*` sobre `Resource: "*"`. Otorgar permisos globales sobre todo el almacenamiento AWS expondría otros buckets de la cuenta y vulneraría las políticas de seguridad.

- **¿Por qué importa el mínimo privilegio específicamente en un sistema distribuido como este?**  
  En un sistema distribuido, múltiples nodos trabajadores ejecutando código en paralelo asumen la misma identidad IAM. Si un rol posee exceso de privilegios, un fallo de software o un worker comprometido puede alterar o eliminar recursos fuera del ámbito del pipeline. En conexión con el **Teorema CAP**, el mínimo privilegio protege la frontera de aislamiento del sistema: así como un nodo inconsistente rompe las garantías de un sistema distribuido **CP**, un rol sobre-privilegiado rompe las garantías de aislamiento e integridad que el resto del Data Lake asume que se sostienen.

---

## 4. Decisiones de EMR

- **Tipo de instancia elegido y justificación:**  
  Se utilizó el tipo de instancia **`m4.large`** (2 vCPUs, 8 GB RAM) con **1 nodo Master + 1 nodo Core**. Esta es la configuración **mínima viable** para ejecutar PySpark distribuido en YARN dentro de las restricciones de vCPU de AWS Learner Lab. Para un entorno de producción, se migraría a instancias de la familia computacional o de memoria (`c5.2xlarge` / `r5.2xlarge`) con autoscaling de nodos Core/Task.

- **Configuración de Spark/aplicaciones instaladas:**  
  Se aprovisionó EMR release `emr-6.15.0` con las aplicaciones **Spark 3.x**, **Hadoop/YARN** y **JupyterHub**.

---

## 5. Estimación de costo

| Escenario | Costo estimado |
|---|---|
| Clúster encendido 24/7 durante un mes | ~ **$360.00 USD** (2 instancias `m4.large` a ~$0.25/hr por nodo = ~$0.50/hr x 720 hrs) |
| Clúster encendido solo durante las ~3 horas del lab | ~ **$1.50 USD** (protegiendo el presupuesto de $50 USD de AWS Academy) |

---

## 6. Reflexión — la era agéntica

Durante la ejecución del laboratorio, la mayor duda radicaba en la configuración precisa de los parámetros de `aws emr create-cluster` compatibles con las cuotas de vCPU de AWS Learner Lab (`m4.large`). El agente asistió en la automatización de la CLI, la corrección de formato del archivo PEM y la verificación agéntica del PySpark job en el máster.

---

## 7. Bitácora de delegación y trabajo en equipo

| Tarea | Integrante Responsable | ¿Delegado a agente? | Herramienta / Justificación |
|---|---|---|---|
| Setup S3 y Estructura Medallion | **Samuel** | No | Creación del bucket `st1630-msanzm-2026` y prefijos `bronze/`, `silver/`, `gold/`. |
| Generación de datos sintéticos | **Samuel** | Sí | Ejecución de `generar_datos.py` con pandas y pyarrow. |
| Políticas IAM Mínimo Privilegio | **Nathalia** | No | Definición de recurso acotado a `s3://st1630-msanzm-2026/*`. |
| Análisis del Teorema CAP y CAPex | **Nathalia** | No | Justificación de S3 como sistema CP con Consistencia Fuerte. |
| Setup AWS CLI y KeyPair SSH | **Mateo** | Sí | Configuración de credenciales temporales y formato PEM RSA. |
| Aprovisionamiento EMR y PySpark | **Mateo** | Sí | Creación del clúster EMR `j-0889427F9EQIW3P0IGI` y ejecución Spark. |
| Captura Spark UI DAG (Exchange) | **Mateo** | No | Navegación en Spark History Server y captura del nodo `Exchange`. |
