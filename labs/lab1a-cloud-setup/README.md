# Lab 1a · Setup cloud: datalake medallion en AWS

**Curso:** ST1630-2026-2 · **Semana:** S4 · **Última edición:** 2026-07-30
**Peso:** parte del 30% de laboratorios (ver `../../docs/evaluacion.md`)

## Objetivo

Construir un datalake mínimo pero real en AWS (S3 con estructura
Bronze/Silver/Gold, un rol IAM de mínimo privilegio y un clúster EMR)
y verificar con PySpark que puedes leerlo de punta a punta. Es tu
primera infraestructura cloud del semestre — el Lab 1b (S5) construye
el pipeline batch completo sobre esta misma base.

**Tiempo estimado:** 4-6 horas, distribuidas en 3 días (ver el
cronograma sugerido en cada parte).
**Entrega:** antes del inicio de la sesión de S5.

## Prerequisitos

- Cuenta **AWS Academy** activa, con la invitación de USD 50 de
  créditos ya recibida en tu correo `@eafit.edu.co` (revisa spam si no
  te llegó; avísale al profesor antes del Día 1 si sigue sin aparecer
  — sin esto no puedes empezar).
- **Python 3.9+** instalado localmente.
- **AWS CLI v2** instalada (no v1 — los comandos de este lab asumen v2).
  Verifica con `aws --version`; instálala desde la
  [guía oficial](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
  si no la tienes.
- Repositorio del curso clonado desde S1 (ver `../../README.md`).
- Haber visto la clase completa de S4 (Teorema CAP, formatos
  columnares, arquitectura medallion, límites del DW clásico).

## Contexto: qué estás construyendo y por qué

```
                    tu cuenta AWS Academy
   ┌─────────────────────────────────────────────────────┐
   │                                                       │
   │   S3: s3://st1630-{tu-usuario}-{año}                 │
   │   ┌───────────┐   ┌───────────┐   ┌───────────┐      │
   │   │  bronze/  │   │  silver/  │   │   gold/   │      │
   │   │  (crudo)  │   │ (limpio)  │   │(agregado) │      │
   │   └───────────┘   └───────────┘   └───────────┘      │
   │         ▲                                             │
   │         │ lee/escribe (solo este bucket)               │
   │         │                                             │
   │   ┌─────┴─────────────────────────┐                  │
   │   │  Rol IAM (mínimo privilegio)   │                  │
   │   │  GetObject/PutObject/          │                  │
   │   │  DeleteObject/ListBucket       │                  │
   │   │  -- solo sobre TU bucket --    │                  │
   │   └─────┬───────────────────────┬─┘                  │
   │         │ asume                 │                    │
   │   ┌─────┴──────┐          ┌─────┴──────┐              │
   │   │ EMR master │◄────────►│  EMR core  │  clúster EMR │
   │   │ (m5.xlarge)│  Spark   │ (m5.xlarge)│              │
   │   └────────────┘          └────────────┘              │
   │                                                       │
   └─────────────────────────────────────────────────────┘
```

Estás construyendo la versión mínima de un datalake medallion: un
bucket S3 con las tres capas vistas en clase, un rol IAM que le da al
clúster EMR exactamente los permisos que necesita (ni uno más), y un
clúster EMR con Spark para leer ese datalake. Es deliberadamente
pequeño — 10.000 filas, un clúster de 2 nodos — porque el objetivo de
este lab no es el volumen de datos, sino que cada pieza de la
arquitectura esté bien justificada.

Esto conecta directamente con el Teorema CAP visto en S4. **S3 con
replicación entre múltiples zonas de disponibilidad es una decisión
CP**: cuando escribes un objeto, S3 no confirma la escritura hasta que
está durablemente replicado, y una lectura después de una escritura
exitosa siempre ve la versión más reciente (consistencia de lectura
tras escritura, garantizada desde 2020). Ante una partición de red
entre zonas, S3 prioriza no servir una versión desactualizada del
objeto antes que sacrificar esa consistencia — el costo es una
disponibilidad de escritura levemente menor que la que tendría un
sistema puramente AP. El **IAM de mínimo privilegio**, por su parte,
no es una decisión CAP en sí misma, pero protege la misma propiedad
que CAP formaliza para los datos: así como un nodo con más autoridad de
la que le corresponde puede romper la garantía de consistencia que el
resto del sistema asume, un rol con más permisos de los que necesita
puede romper la garantía de aislamiento que el resto de tu cuenta (y de
la organización de AWS Academy) asume que existe entre recursos. Vas a
ver esta misma idea, más desarrollada, en la Parte 3.

## Parte 1: Setup inicial de AWS (Día 1 — 60 min)

### 1.1 Activar créditos y acceder a la consola

1. Revisa tu correo `@eafit.edu.co` por la invitación de **AWS
   Academy** y acéptala.
2. Entra al **Learner Lab** desde el portal de AWS Academy y haz clic
   en **Start Lab**. Espera a que el ícono pase a verde.
3. Haz clic en **AWS Details → Show** para ver las credenciales
   temporales (`AWS Access Key ID`, `AWS Secret Access Key`,
   `AWS Session Token`) — las vas a necesitar en el siguiente paso.

**Error frecuente:** las credenciales de AWS Academy **expiran cada
~4 horas** y al reiniciar el Learner Lab cambian por completo (no solo
se renuevan). Si un comando que antes funcionaba empieza a fallar con
`ExpiredToken` o `InvalidClientTokenId`, vuelve al Learner Lab, copia
las credenciales nuevas y repite el paso 1.2.

### 1.2 Instalar y configurar AWS CLI

```bash
aws --version
# Salida esperada: aws-cli/2.x.x Python/3.x.x <tu-SO>/... — si dice
# aws-cli/1.x.x, desinstala y reinstala la v2 (ver enlace en Prerequisitos).
```

Configura el perfil con las credenciales del Learner Lab. AWS Academy
entrega tres valores (incluye un **session token**, a diferencia de una
cuenta AWS normal), así que la configuración se hace en dos comandos:

```bash
aws configure
# AWS Access Key ID [None]: <pega tu Access Key>
# AWS Secret Access Key [None]: <pega tu Secret Key>
# Default region name [None]: us-east-1
# Default output format [None]: json

aws configure set aws_session_token <pega tu Session Token>
```

### 1.3 Verificar que la CLI funciona

```bash
aws sts get-caller-identity
```

**Output esperado:**

```json
{
    "UserId": "AROAEXAMPLE:user123456",
    "Account": "123456789012",
    "Arn": "arn:aws:sts::123456789012:assumed-role/voclabs/user123456"
}
```

Si ves un JSON parecido (con tu propio `Account` y `Arn`), la CLI está
bien configurada. Guarda el valor de `Account` — lo vas a usar en la
Parte 3.

**Error frecuente:** `Unable to locate credentials`. Casi siempre
significa que el paso `aws configure set aws_session_token` no se
ejecutó, o se ejecutó con un espacio o salto de línea pegado dentro del
token. Vuelve a copiar el token completo desde el Learner Lab sin
espacios extra.

### 1.4 Crear un key pair para EMR

Vas a necesitar un key pair de EC2 para poder conectarte por SSH al
clúster en la Parte 5.

```bash
aws ec2 create-key-pair \
    --key-name st1630-lab1a \
    --query 'KeyMaterial' \
    --output text > st1630-lab1a.pem

chmod 400 st1630-lab1a.pem
```

**Output esperado:** el comando no imprime nada en pantalla (la salida
se redirige al archivo); verifica con `ls -la st1630-lab1a.pem` que el
archivo existe y no está vacío.

**Error frecuente:** `InvalidKeyPair.Duplicate` si ya tenías un key
pair con ese nombre de un intento anterior. Usa
`aws ec2 delete-key-pair --key-name st1630-lab1a` primero, o cambia el
nombre (y recuerda usar el mismo nombre en `create_emr.sh`, variable
`KEY_NAME`).

> Guarda `st1630-lab1a.pem` fuera del repositorio (por ejemplo, en tu
> carpeta personal `~/.ssh/`) — es una credencial, nunca debe llegar a
> un commit.

## Parte 2: Crear el datalake en S3 (Día 1 — 45 min)

Todo este paso está automatizado en
[`scripts/setup_s3.sh`](scripts/setup_s3.sh). Ábrelo, lee los
comentarios (explican **qué** hace cada bloque y **por qué**), edita
las variables marcadas `# EDITAR ANTES DE EJECUTAR`, y ejecútalo:

```bash
python3 datos/generar_datos.py     # genera los archivos de prueba primero
bash scripts/setup_s3.sh
```

El script:

1. Crea el bucket `st1630-{tu-usuario}-{año}` (idempotente — si ya
   existe, no falla).
2. Bloquea todo acceso público al bucket.
3. Crea los prefijos `bronze/`, `silver/`, `gold/`.
4. Sube `prueba_parquet.parquet` y `prueba_csv.csv` a `bronze/ventas/`.
5. Verifica la estructura completa con `aws s3 ls --recursive`.

**Verifica:** la salida final del script lista los 5 objetos
(`bronze/`, `silver/`, `gold/`, y los dos archivos dentro de
`bronze/ventas/`) con su tamaño.

**Error frecuente:** `BucketAlreadyExists` (sin el sufijo `ByYou`) —
significa que el nombre de bucket ya lo tiene **otra cuenta** de AWS en
el mundo (los nombres de bucket son únicos globalmente). Cambia el
valor de `ESTUDIANTE` en el script a algo más específico (tu usuario
institucional completo, no solo tu nombre).

**Qué puedes delegar aquí:** el boilerplate del script ya está hecho —
no hace falta que un agente te lo reescriba. Sí puedes delegar
troubleshooting si algún comando de la CLI falla por una razón que no
reconoces. **Qué debes hacer a mano:** decidir el nombre final de tu
bucket y confirmar que la estructura de prefijos coincide con lo que
vas a documentar en `plantillas/architecture.md`.

## Parte 3: Configurar IAM con mínimo privilegio (Día 2 — 60 min)

**Esta es la parte más importante pedagógicamente del lab.** Ejecuta
[`scripts/setup_iam.sh`](scripts/setup_iam.sh) (edita las mismas
variables `ESTUDIANTE`/`ANIO` que usaste en el paso anterior):

```bash
bash scripts/setup_iam.sh
```

El script crea el rol `EMR_EC2_{tu-usuario}_role`, su instance profile,
y adjunta esta política (la versión **correcta**, de mínimo
privilegio):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AccesoObjetosBucketPropio",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::st1630-{tu-usuario}-{año}/*"
    },
    {
      "Sid": "ListarBucketPropio",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::st1630-{tu-usuario}-{año}"
    }
  ]
}
```

Compárala contra el **error típico** que el script también imprime
(pero **no aplica**) — nota el `"Resource": "*"`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "NO_USAR_excesivamente_permisivo",
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": "*"
    }
  ]
}
```

Esa segunda versión le daría al clúster acceso de lectura/escritura/
borrado sobre **cualquier bucket accesible desde tu cuenta**, no solo
el tuyo. El script termina verificando con
`aws iam simulate-principal-policy` que tu rol sí puede escribir en tu
propio bucket, y que **no** tiene permiso para borrar otro bucket de la
cuenta.

**Verifica:** la primera simulación debe devolver `allowed`; la segunda,
`implicitDeny`. Si la primera da `implicitDeny`, revisa que el nombre
del bucket en la política coincida exactamente con el que creaste en
la Parte 2 (mayúsculas/minúsculas y guiones incluidos).

**Error frecuente:** `EntityAlreadyExists` al crear la política, si
corriste el script dos veces con variables distintas — el script ya
maneja esto como idempotente si las variables son las mismas; si
cambiaste `ESTUDIANTE` a mitad de camino, vas a tener recursos huérfanos
de la ejecución anterior. Usa siempre las mismas variables en los tres
scripts.

## Parte 4: Aprovisionar el clúster EMR (Día 2 — 45 min)

Antes de ejecutar, edita en
[`scripts/create_emr.sh`](scripts/create_emr.sh) las variables
`KEY_NAME` (el key pair de la Parte 1.4) y `SUBNET_ID` (una subnet de
tu VPC por defecto — consíguela con
`aws ec2 describe-subnets --filters "Name=default-for-az,Values=true" --query 'Subnets[0].SubnetId' --output text`).

```bash
bash scripts/create_emr.sh
```

El comando central que ejecuta el script (ya con tus variables
resueltas) es:

```bash
aws emr create-cluster \
    --name "st1630-{tu-usuario}-emr" \
    --release-label emr-6.15.0 \
    --applications Name=Spark Name=Hadoop \
    --instance-type m5.xlarge \
    --instance-count 2 \
    --use-default-roles \
    --ec2-attributes "KeyName={tu-key},InstanceProfile=EMR_EC2_{tu-usuario}_profile,SubnetId={tu-subnet}" \
    --log-uri "s3://st1630-{tu-usuario}-{año}/logs/" \
    --bootstrap-actions "Path=s3://.../bootstrap.sh,Name=Instalar dependencias Python" \
    --region us-east-1
```

`--instance-count 2` con instancias `m5.xlarge` crea automáticamente
1 nodo master + 1 nodo core — la configuración mínima viable para que
Spark corra en modo distribuido real (no en modo local de un solo
proceso).

**Verifica** que el clúster llegó a estado `WAITING` (puede tardar
8-12 minutos):

```bash
aws emr describe-cluster --cluster-id <tu-cluster-id> --query 'Cluster.Status.State' --output text
```

**Error frecuente:** el clúster queda en `TERMINATED_WITH_ERRORS` casi
de inmediato — casi siempre es un `SubnetId` inválido o de una zona de
disponibilidad sin capacidad para `m5.xlarge`. Revisa el mensaje
completo con
`aws emr describe-cluster --cluster-id <id> --query 'Cluster.Status.StateChangeReason'`
y, si el problema es de capacidad, prueba con otra subnet de tu VPC
por defecto.

> ### ⚠️ Apaga el clúster cuando no lo uses
>
> Un clúster de 1 master + 1 core (`m5.xlarge`) cuesta aproximadamente
> **USD 0.38-0.50 por hora** solo en cómputo (tarifas aproximadas de
> `us-east-1` — verifica el precio vigente en
> [calculator.aws](https://calculator.aws/)). Dejarlo encendido un día
> completo por olvido puede costar más de USD 10 — y tus créditos de
> AWS Academy son **USD 50 para todo el semestre**.
>
> ```bash
> aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1
> ```
>
> Apágalo en cuanto termines la Parte 5. Este mismo comando aparece de
> nuevo, en un bloque igual de visible, dentro de `create_emr.sh`.

## Parte 5: Verificación — Spark lee tu datalake (Día 3 — 60 min)

1. Conéctate al clúster: desde **EMR Studio** (recomendado — no
   requiere SSH) crea un Workspace y adjúntalo a tu clúster, o
   conéctate por SSH al nodo master:

   ```bash
   ssh -i st1630-lab1a.pem hadoop@<master-public-dns>
   ```

2. Abre `notebooks/verificacion.ipynb` (súbelo a tu Workspace de EMR
   Studio, o cópialo al master si trabajas por SSH + Jupyter).
3. Edita la variable `BUCKET` en la Celda 2 con el nombre de tu bucket.
4. Ejecuta las celdas en orden. La Celda 2 lee el Parquet de Bronze y
   muestra su schema + 5 filas; la Celda 3 corre el benchmark Parquet
   vs. CSV (la misma comparación de la Parte 5 del slide 10 de S4).
5. Completa el análisis de la Celda 4 y sigue las instrucciones de la
   Celda 5 para capturar el DAG en Spark UI.

**Verifica:** si la Celda 2 muestra el schema y las 5 filas sin error,
tu datalake funciona correctamente de punta a punta (bucket, permisos
IAM y clúster, los tres a la vez). Es la señal más fuerte de que el lab
está bien hecho.

**Error frecuente:** `AccessDenied` al leer desde S3 dentro de Spark —
casi siempre significa que el clúster se creó con el `InstanceProfile`
equivocado (revisa que `create_emr.sh` haya usado
`EMR_EC2_{tu-usuario}_profile`, no `EMR_EC2_DefaultRole`). Si ocurre,
no hace falta recrear el clúster completo: puedes verificar el
instance profile asociado con
`aws emr describe-cluster --cluster-id <id> --query 'Cluster.Ec2InstanceAttributes.IamInstanceProfile'`.

**No olvides apagar el clúster** (`aws emr terminate-clusters ...`) en
cuanto termines este paso.

## Ejercicios de reto (para ir más allá)

Opcionales — no son obligatorios para la nota, pero profundizan justo
los conceptos que más preguntas generan en el Parcial 1 (S8).

1. **Versionado S3:** activa el versionado en tu bucket
   (`aws s3api put-bucket-versioning`). ¿Qué problema resuelve esto en
   términos del Teorema CAP? (pista: piensa en qué pasa si dos
   procesos escriben el mismo objeto casi al mismo tiempo).
2. **Política de solo lectura:** crea una segunda política IAM que solo
   tenga `s3:GetObject` y `s3:ListBucket` (sin `PutObject` ni
   `DeleteObject`), simúlala con `iam simulate-principal-policy`, y
   confirma que un intento de escritura en `bronze/` da `implicitDeny`.
3. **Costo real vs. alternativa:** usa la
   [calculadora de AWS](https://calculator.aws/) para estimar el costo
   mensual de tu datalake actual (S3 + EMR encendido solo durante tus
   sesiones de trabajo) y compáralo con mantener el mismo volumen de
   datos en una instancia EC2 con disco EBS corriendo 24/7. ¿Cuál sale
   más barato para un dataset de este tamaño, y por qué cambia la
   respuesta si el dataset creciera 100x?

## Entregable y rúbrica

### Qué va en el PR

```
labs/lab1a-cloud-setup/
└── entregas/
    └── <tu-usuario>/
        ├── architecture.md         # copiado de plantillas/ y completado
        ├── dag_spark_ui.png        # captura indicada en la Celda 5 del notebook
        └── verificacion_ejecutado.ipynb  # tu copia del notebook, con outputs visibles
```

No subas tus credenciales de AWS ni el archivo `.pem` del key pair —
ninguno de los dos debe aparecer en el PR bajo ninguna circunstancia.

### Rúbrica

| Criterio | Peso | Completo | Parcial | Incompleto |
|---|---|---|---|---|
| **Infraestructura** (S3 + IAM + EMR funcionando) | 30% | El bucket, el rol IAM y el clúster existen con la configuración pedida; el clúster llegó a `WAITING` y se apagó correctamente después | Falta alguna pieza menor (p. ej. el bucket no bloquea acceso público, o el clúster se apagó pero mucho después de terminar) | El clúster nunca llegó a `WAITING`, o la política IAM usa `Resource: "*"` |
| **Verificación Spark** | 25% | El notebook corre de principio a fin, muestra el schema/filas del Parquet leído desde S3, y el benchmark con tiempos reales | Corre pero con algún paso incompleto (p. ej. sin la captura del DAG) | No hay evidencia de haber ejecutado el notebook contra el clúster real |
| **`architecture.md`** | 25% | Las 7 secciones completas, con justificaciones específicas a la propia ejecución (no genéricas) y la sección de IAM bien argumentada | Completo pero con justificaciones vagas, o falta la estimación de costo | Plantilla sin completar, o respuestas genéricas sin adaptar a la propia ejecución |
| **Bitácora de delegación** | 20% | Completa, con justificación específica por fila, consistente con lo que realmente se delegó | Completa pero genérica | Ausente, o marca como "no delegado" tareas que evidentemente sí lo fueron |

## Bitácora de delegación

Este lab sigue `../../docs/politica-ia.md`. Tareas específicas de este
lab y qué aplica por defecto:

| Tarea | ¿Se puede delegar? | Nota |
|---|---|---|
| Generar/editar el boilerplate de los scripts bash | Sí | Ya viene resuelto en el repo; delegar ajustes menores está permitido |
| Troubleshooting de errores de AWS CLI / EMR / IAM | Sí | Bajo valor de aprendizaje memorizar mensajes de error de la CLI |
| Formatear o pulir la redacción de `architecture.md` | Sí | Redacción, no contenido |
| Decidir qué permisos IAM otorgar (Parte 3) | **No** | Es la decisión de diseño central del lab |
| Diseñar la estructura de prefijos Bronze/Silver/Gold | **No** | Decisión de arquitectura que se evalúa en `architecture.md` |
| Escribir las justificaciones de `architecture.md` | **No** | Debe reflejar tu propio razonamiento, no el de un agente |
| Interpretar los resultados de Spark (benchmark, DAG) | **No** | Es evidencia empírica de tu propia ejecución — un agente no tiene acceso a tu Spark UI real |

## Troubleshooting

| # | Error / síntoma | Causa probable | Solución |
|---|---|---|---|
| 1 | `ExpiredToken` / `InvalidClientTokenId` en cualquier comando `aws` | Las credenciales de AWS Academy expiraron (~4 horas) o el Learner Lab se reinició | Vuelve al Learner Lab, copia las credenciales nuevas y repite `aws configure` + `aws configure set aws_session_token` |
| 2 | `AccessDenied` al leer/escribir en S3 desde Spark o desde la CLI | El rol/usuario no tiene los permisos IAM correctos, o el `InstanceProfile` del clúster EMR no es el que creaste en `setup_iam.sh` | Revisa la política adjunta al rol (`aws iam list-attached-role-policies --role-name EMR_EC2_{tu-usuario}_role`) y que el clúster use ese instance profile |
| 3 | Clúster EMR en estado `TERMINATED_WITH_ERRORS` | `SubnetId` inválido, zona sin capacidad para `m5.xlarge`, o `KeyName` inexistente | Revisa `Cluster.Status.StateChangeReason` con `describe-cluster`; prueba otra subnet o verifica el nombre del key pair |
| 4 | PySpark no encuentra el archivo en S3 (`Path does not exist`) | Ruta mal escrita, o el bucket/prefijo no coincide con lo que subiste en la Parte 2 | Verifica con `aws s3 ls s3://<bucket>/bronze/ventas/` que el archivo existe exactamente en esa ruta |
| 5 | El clúster está muy lento o el job nunca termina | Solo 1 core node y el dataset/consulta pide más paralelismo del disponible, o el bootstrap action falló silenciosamente | Para este lab (10.000 filas) no debería ocurrir — si pasa, revisa los logs del bootstrap en `s3://<bucket>/logs/` antes de asumir que necesitas más nodos |
| 6 | SSH al nodo master falla (`Connection timed out`) | El security group del clúster no tiene el puerto 22 abierto desde tu IP | Desde la consola EC2, agrega una regla de entrada al security group `ElasticMapReduce-master` permitiendo SSH (22) desde tu IP actual |
| 7 | Un agente de IA generó una política IAM con `"Resource": "*"` o `"Action": "s3:*"` | Es el patrón por defecto que muchos agentes proponen si no se les pide explícitamente mínimo privilegio | Revisa siempre el JSON antes de aplicarlo (compáralo contra el bloque "NO_USAR" de `setup_iam.sh`); si ves `*` en `Resource` o en `Action` de un statement `Allow`, no lo apliques sin acotar el recurso |
| 8 | Costos inesperados en la cuenta de AWS Academy | Un clúster EMR (o una instancia EC2 suelta) quedó encendido más tiempo del planeado | Revisa el dashboard de créditos del Learner Lab regularmente durante el lab; si ves consumo inesperado, verifica primero `aws emr list-clusters --active` y termina lo que encuentres encendido |

## Referencias

- [AWS EMR — Documentación oficial](https://docs.aws.amazon.com/emr/)
- [AWS S3 — Documentación oficial](https://docs.aws.amazon.com/s3/)
- [AWS IAM — Documentación oficial](https://docs.aws.amazon.com/iam/)
- [AWS Pricing Calculator](https://calculator.aws/)
- Kleppmann, *Designing Data-Intensive Applications* — caps. 5 (replicación) y 9 (consistencia y consenso)
- Slides de la clase S4 del curso (arquitectura medallion, CAP, Parquet vs. CSV)
