#!/usr/bin/env bash
# setup_iam.sh — Lab 1a (ST1630-2026-2, S4)
#
# Crea el rol IAM que usará el clúster EMR para leer/escribir en TU
# bucket S3, con una política de MÍNIMO PRIVILEGIO: solo los permisos
# que EMR realmente necesita, solo sobre tu bucket -- nunca "*".
#
# Esta es la parte más importante del lab a nivel pedagógico. Un rol
# con Resource: "*" es exactamente el tipo de exceso de privilegio que
# convierte un incidente pequeño (una credencial filtrada, un bug en
# un job) en una brecha de todo el datalake de la cuenta.
#
# Qué puedes delegar a un agente: el boilerplate de create-role /
# create-policy (ya está hecho). Qué debes decidir a mano: qué acciones
# van en la política, sobre qué recurso, y por qué NO agregar más de lo
# que EMR necesita (documenta la decisión en ../plantillas/architecture.md).

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR (debe coincidir con setup_s3.sh)
# ─────────────────────────────────────────────────────────────
ESTUDIANTE="tu-usuario"      # EDITAR: el mismo valor que usaste en setup_s3.sh
ANIO="2026"                  # EDITAR si tu cohorte no es 2026
# ─────────────────────────────────────────────────────────────

BUCKET_NAME="st1630-${ESTUDIANTE}-${ANIO}"
ROLE_NAME="EMR_EC2_${ESTUDIANTE}_role"
PROFILE_NAME="EMR_EC2_${ESTUDIANTE}_profile"
POLICY_NAME="st1630-${ESTUDIANTE}-s3-min-privilegio"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Rol objetivo: ${ROLE_NAME} (bucket: s3://${BUCKET_NAME})"

# 1. Trust policy: quién puede asumir este rol. EMR asigna este rol a
#    las instancias EC2 del clúster (master + core), así que el
#    "Principal" que confía es el servicio EC2, no un usuario humano.
cat > "${TMP_DIR}/trust-policy.json" <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ec2.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# 2. Crear el rol -- idempotente: si ya existe, se reutiliza.
if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
    echo "El rol ${ROLE_NAME} ya existe -- se omite la creación."
else
    echo "Creando rol ${ROLE_NAME}..."
    aws iam create-role \
        --role-name "${ROLE_NAME}" \
        --assume-role-policy-document "file://${TMP_DIR}/trust-policy.json" >/dev/null
fi

# 3. Crear el instance profile y asociar el rol -- EMR adjunta permisos
#    a las instancias EC2 a través de un instance profile, no del rol
#    directamente.
if aws iam get-instance-profile --instance-profile-name "${PROFILE_NAME}" >/dev/null 2>&1; then
    echo "El instance profile ${PROFILE_NAME} ya existe -- se omite la creación."
else
    echo "Creando instance profile ${PROFILE_NAME}..."
    aws iam create-instance-profile --instance-profile-name "${PROFILE_NAME}" >/dev/null
    aws iam add-role-to-instance-profile \
        --instance-profile-name "${PROFILE_NAME}" \
        --role-name "${ROLE_NAME}"
    # El instance profile tarda unos segundos en propagarse en IAM.
    sleep 10
fi

# 4. Política de MÍNIMO PRIVILEGIO -- solo lectura/escritura/borrado de
#    objetos, solo sobre TU bucket. s3:ListBucket va aparte porque actúa
#    sobre el bucket como recurso, no sobre los objetos dentro de él
#    (por eso el Resource del segundo statement es el bucket, sin "/*").
cat > "${TMP_DIR}/policy-correcta.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AccesoObjetosBucketPropio",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME}/*"
    },
    {
      "Sid": "ListarBucketPropio",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${BUCKET_NAME}"
    }
  ]
}
EOF

# ─────────────────────────────────────────────────────────────
# CONTRASTE PEDAGÓGICO -- el error típico (NO se aplica, solo se
# guarda para que lo compares en la Parte 3 del lab). Nota el "*" en
# Resource: esto le daría a EMR permiso de leer/escribir/borrar objetos
# en CUALQUIER bucket de CUALQUIER cuenta de AWS que le otorgue acceso
# público -- exactamente lo que el mínimo privilegio existe para evitar.
# ─────────────────────────────────────────────────────────────
cat > "${TMP_DIR}/policy-incorrecta-NO-USAR.json" <<'EOF'
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
EOF
echo "Referencia guardada (NO aplicada): ${TMP_DIR}/policy-incorrecta-NO-USAR.json"
cat "${TMP_DIR}/policy-incorrecta-NO-USAR.json"
echo

# 5. Crear (o reutilizar) la política y adjuntarla al rol.
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

if aws iam get-policy --policy-arn "${POLICY_ARN}" >/dev/null 2>&1; then
    echo "La política ${POLICY_NAME} ya existe -- se omite la creación."
else
    echo "Creando política ${POLICY_NAME}..."
    aws iam create-policy \
        --policy-name "${POLICY_NAME}" \
        --policy-document "file://${TMP_DIR}/policy-correcta.json" >/dev/null
fi

echo "Adjuntando política al rol..."
aws iam attach-role-policy --role-name "${ROLE_NAME}" --policy-arn "${POLICY_ARN}"

# 6. Verificar con el simulador de IAM que el rol SÍ puede escribir en
#    tu bucket y que NO tiene permisos fuera de lo esperado. Esto
#    reemplaza la prueba manual de "crear el clúster y ver si falla" --
#    puedes detectar un error de permisos en segundos, sin gastar
#    crédito de cómputo.
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

echo
echo "=== Simulación: ¿puede escribir en su propio bucket? (debe ser 'allowed') ==="
aws iam simulate-principal-policy \
    --policy-source-arn "${ROLE_ARN}" \
    --action-names "s3:PutObject" \
    --resource-arns "arn:aws:s3:::${BUCKET_NAME}/bronze/test.txt" \
    --query 'EvaluationResults[0].EvalDecision' --output text

echo
echo "=== Simulación: ¿puede borrar OTRO bucket de la cuenta? (debe ser 'implicitDeny') ==="
aws iam simulate-principal-policy \
    --policy-source-arn "${ROLE_ARN}" \
    --action-names "s3:DeleteBucket" \
    --resource-arns "arn:aws:s3:::${BUCKET_NAME}" \
    --query 'EvaluationResults[0].EvalDecision' --output text

echo
echo "Listo. Instance profile para EMR: ${PROFILE_NAME}"
echo "Siguiente paso: scripts/create_emr.sh (Parte 4 del lab)."