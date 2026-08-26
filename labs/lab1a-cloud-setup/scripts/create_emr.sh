#!/usr/bin/env bash
# create_emr.sh — Lab 1a (ST1630-2026-2, S4)
#
# Crea el clúster EMR (1 master + 1 core, m5.xlarge, Spark 3.x) que vas
# a usar en la Parte 5 del lab para leer tu datalake desde PySpark.
#
# ### ================================================================
# ### LEE ESTO PRIMERO: un clúster EMR cobra por hora mientras esté
# ### encendido, incluso si no lo estás usando. Al final de este script
# ### encontrarás el comando exacto para APAGARLO -- ejecútalo apenas
# ### termines la Parte 5. Ver también la Parte 4 de ../README.md.
# ### ================================================================
#
# Uso:
#   1. Edita las variables de la sección "EDITAR ANTES DE EJECUTAR"
#      (necesitas el KeyName de tu key pair y un SubnetId de tu VPC
#      por defecto -- ver Parte 1 del lab para ambos).
#   2. Ejecuta: bash create_emr.sh
#
# Qué puedes delegar a un agente: el boilerplate de create-cluster (ya
# está hecho) y el troubleshooting si el clúster falla al arrancar. Qué
# debes decidir a mano: el tipo de instancia y por qué es "mínimo
# viable" para este ejercicio, no para producción (documéntalo en
# ../plantillas/architecture.md).

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR (ESTUDIANTE/ANIO deben coincidir con los
# scripts anteriores; KEY_NAME y SUBNET_ID son obligatorios y no
# tienen un valor por defecto válido)
# ─────────────────────────────────────────────────────────────
ESTUDIANTE="samedinac"        # EDITAR: el mismo valor que en setup_s3.sh / setup_iam.sh
ANIO="2023"                    # EDITAR si tu cohorte no es 2026
REGION="us-east-1"             # EDITAR: la misma región que usaste en setup_s3.sh
KEY_NAME="st1630-lab1a"   # EDITAR: el key pair que creaste en la Parte 1
SUBNET_ID="subnet-06b81c6da3d161a97"       # EDITAR: una subnet de tu VPC por defecto
# ─────────────────────────────────────────────────────────────

BUCKET_NAME="st1630-${ESTUDIANTE}-${ANIO}"
PROFILE_NAME="EMR_EC2_DefaultRole"
CLUSTER_NAME="st1630-${ESTUDIANTE}-emr"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

# 1. Asegurar que existan los roles de servicio por defecto de EMR
#    (EMR_DefaultRole). Esto es distinto del rol EC2 que usarán los nodos
#    para acceder a tus datos en S3 (${PROFILE_NAME}).
echo "Verificando roles de servicio por defecto de EMR..."
aws emr create-default-roles >/dev/null 2>&1 || true

# 2. Bootstrap action: instala pandas y pyarrow en cada nodo del
#    clúster antes de que arranque Spark, para que el notebook de
#    verificación pueda usarlos si lo necesita.
cat > "${TMP_DIR}/bootstrap.sh" <<'EOF'
#!/bin/bash
sudo pip3 install --quiet pandas pyarrow
EOF
aws s3 cp "${TMP_DIR}/bootstrap.sh" "s3://${BUCKET_NAME}/bootstrap/bootstrap.sh"

# 3. Crear el clúster: 1 master + 1 core, ambos m5.xlarge -- la
#    configuración mínima viable para correr Spark en modo distribuido
#    real (si solo pidieras 1 nodo, Spark correría en modo local y no
#    verías nada del comportamiento distribuido visto en clase).
echo "Creando clúster EMR '${CLUSTER_NAME}'..."
CLUSTER_ID=$(aws emr create-cluster \
    --name "${CLUSTER_NAME}" \
    --release-label emr-6.15.0 \
    --applications Name=Spark Name=Hadoop \
    --instance-type m5.xlarge \
    --instance-count 2 \
    --service-role EMR_DefaultRole \
    --ec2-attributes "KeyName=${KEY_NAME},InstanceProfile=${PROFILE_NAME},SubnetId=${SUBNET_ID}" \
    --log-uri "s3://${BUCKET_NAME}/logs/" \
    --bootstrap-actions "Path=s3://${BUCKET_NAME}/bootstrap/bootstrap.sh,Name=Instalar dependencias Python" \
    --region "${REGION}" \
    --query 'ClusterId' --output text)

echo "Clúster creado: ${CLUSTER_ID}"
echo "Esperando a que quede en estado WAITING (puede tardar 8-12 minutos)..."

# 4. Verificar el estado. "WAITING" significa que el clúster está listo
#    para recibir jobs (Parte 5 del lab). No hay que esperar a que
#    termine este script -- puedes revisar el estado en cualquier
#    momento con el mismo comando.
aws emr describe-cluster --cluster-id "${CLUSTER_ID}" --region "${REGION}" \
    --query 'Cluster.Status.State' --output text

cat <<EOF

Para volver a consultar el estado más tarde:
  aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${REGION} --query 'Cluster.Status.State' --output text

Cuando el estado sea WAITING, continúa con la Parte 5 del lab
(../README.md) usando el ID de clúster: ${CLUSTER_ID}

# ================================================================
# ### ADVERTENCIA DE COSTO -- LEE ESTO ANTES DE ALEJARTE DEL TECLADO
# ================================================================
# Este clúster (1 master + 1 core, m5.xlarge) cuesta aproximadamente
# USD 0.38-0.50/hora SOLO en cómputo EC2 on-demand (tarifa aproximada
# de us-east-1 al momento de escribir esto; verifica el precio vigente
# en https://calculator.aws/), más el cargo de EMR y el almacenamiento
# EBS. Dejarlo encendido 24 horas por olvido puede costar más de
# USD 10 -- y tus créditos de AWS Academy son solo USD 50 para todo
# el semestre.
#
# APAGA el clúster en cuanto termines la Parte 5:
#
#   aws emr terminate-clusters --cluster-ids ${CLUSTER_ID} --region ${REGION}
#
# Verifica que sí se apagó (State debe pasar a TERMINATED):
#
#   aws emr describe-cluster --cluster-id ${CLUSTER_ID} --region ${REGION} --query 'Cluster.Status.State' --output text
# ================================================================
EOF
