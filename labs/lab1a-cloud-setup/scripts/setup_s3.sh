#!/usr/bin/env bash
# setup_s3.sh — Lab 1a (ST1630-2026-2, S4)
#
# Crea el bucket S3 del datalake y la estructura de prefijos
# Bronze/Silver/Gold, bloquea el acceso público, y sube los dos
# archivos de prueba de ../datos/.
#
# Uso:
#   1. Edita las variables de la sección "EDITAR ANTES DE EJECUTAR".
#   2. Genera los datos de prueba si aún no existen:
#        python3 ../datos/generar_datos.py
#   3. Ejecuta: bash setup_s3.sh
#
# Qué puedes delegar a un agente: el boilerplate de este script (ya
# está hecho). Qué debes decidir a mano: el nombre del bucket, la
# región, y por qué esa estructura de prefijos tiene sentido para tu
# datalake (documéntalo en ../plantillas/architecture.md).

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
ESTUDIANTE="tu-usuario"      # EDITAR: minúsculas, sin espacios ni tildes (ej. "jperezg")
ANIO="2026"                  # EDITAR si tu cohorte no es 2026
REGION="us-east-1"           # EDITAR: región donde vive tu cuenta AWS Academy
# ─────────────────────────────────────────────────────────────

BUCKET_NAME="st1630-${ESTUDIANTE}-${ANIO}"
DATOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../datos" && pwd)"

echo "Bucket objetivo: s3://${BUCKET_NAME} (región ${REGION})"

# 1. Crear el bucket -- idempotente: si ya existe y es tuyo, no falla.
#    head-bucket devuelve 0 si el bucket existe y tienes acceso; en ese
#    caso saltamos la creación en vez de fallar con BucketAlreadyOwnedByYou.
if aws s3api head-bucket --bucket "${BUCKET_NAME}" 2>/dev/null; then
    echo "El bucket ya existe -- se omite la creación."
else
    echo "Creando bucket..."
    if [ "${REGION}" = "us-east-1" ]; then
        # us-east-1 es la única región que NO acepta LocationConstraint
        # explícito en create-bucket -- particularidad histórica de la API.
        aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${REGION}"
    else
        aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" \
            --create-bucket-configuration LocationConstraint="${REGION}"
    fi
fi

# 2. Bloquear todo acceso público al bucket. Un datalake académico con
#    datos (aunque sean sintéticos) no tiene ninguna razón para ser
#    legible desde internet -- este es el ajuste de seguridad por
#    defecto que se espera de cualquier bucket productivo.
echo "Bloqueando acceso público..."
aws s3api put-public-access-block \
    --bucket "${BUCKET_NAME}" \
    --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# 3. Crear la estructura Bronze/Silver/Gold. S3 no tiene carpetas reales
#    -- son solo prefijos de la key -- pero crear un objeto marcador por
#    capa deja la estructura visible de inmediato en la consola y en
#    `aws s3 ls`, incluso antes de subir datos.
echo "Creando estructura de prefijos (bronze/, silver/, gold/)..."
for capa in bronze silver gold; do
    aws s3api put-object --bucket "${BUCKET_NAME}" --key "${capa}/" >/dev/null
done

# 4. Subir los archivos de prueba a Bronze (datos crudos, tal como se
#    generaron -- Bronze nunca se transforma en el origen).
echo "Subiendo archivos de prueba a bronze/ventas/..."
aws s3 cp "${DATOS_DIR}/prueba_parquet.parquet" "s3://${BUCKET_NAME}/bronze/ventas/prueba_parquet.parquet"
aws s3 cp "${DATOS_DIR}/prueba_csv.csv" "s3://${BUCKET_NAME}/bronze/ventas/prueba_csv.csv"

# 5. Verificación final: estructura completa y tamaño total.
echo
echo "=== Estructura del datalake (s3://${BUCKET_NAME}) ==="
aws s3 ls "s3://${BUCKET_NAME}" --recursive --human-readable --summarize

echo
echo "Listo. Bucket: s3://${BUCKET_NAME}"
echo "Siguiente paso: scripts/setup_iam.sh (Parte 3 del lab)."
