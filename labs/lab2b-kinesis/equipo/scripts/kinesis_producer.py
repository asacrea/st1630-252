"""kinesis_producer.py — Lab 2b, Parte 4 (ST1630-2026-2, S7) — OPCIONAL

Envía 500 pedidos sintéticos a un stream de Kinesis Data Streams. Es
el equivalente en AWS de ../../lab2a-kafka/scripts/productor_kafka.py
-- misma decisión de diseño (particionar por región), transporte
distinto.

Este script viene resuelto (no tiene TODO): el objetivo de la Parte 4
es que compares la equivalencia de conceptos (ver Pregunta 7 de
streaming_design.md), no que reimplementes una decisión que ya
tomaste y justificaste en el Lab 2a.

Prerequisito: el stream debe existir (Parte 4.2 del README):
    aws kinesis create-stream --stream-name pedidos-ventas-kinesis \
        --shard-count 2 --region us-east-1

Uso:
    python3 kinesis_producer.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime

import boto3

REGION = os.environ.get("KINESIS_REGION", "us-east-1")
STREAM = os.environ.get("KINESIS_STREAM", "pedidos-ventas-kinesis")
N_PEDIDOS = 500

kinesis = boto3.client("kinesis", region_name=REGION)

REGIONES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Otro"]
PESOS = [0.40, 0.20, 0.15, 0.10, 0.15]
CATEGORIAS = ["Electrónica", "Ropa", "Alimentos"]


def generar_pedido() -> dict:
    region = random.choices(REGIONES, weights=PESOS, k=1)[0]
    return {
        "pedido_id": str(uuid.uuid4()),
        "region": region,
        "categoria": random.choice(CATEGORIAS),
        "total": round(random.uniform(15_000, 2_500_000), 2),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "devuelto": random.random() < 0.07,
    }


def main():
    print(f"Enviando {N_PEDIDOS} pedidos a Kinesis stream '{STREAM}'...")

    for i in range(N_PEDIDOS):
        pedido = generar_pedido()

        # boto3 put_record()  ≈  kafka producer.send() del Lab 2a
        # PartitionKey        ≈  key del mensaje Kafka
        #   -- misma decisión de diseño (key=region) que en el Lab 2a:
        #      hash(region) % N_shards → misma región, mismo shard →
        #      orden garantizado POR REGIÓN dentro del shard.
        response = kinesis.put_record(
            StreamName=STREAM,
            Data=json.dumps(pedido).encode("utf-8"),
            PartitionKey=pedido["region"],
        )

        # ShardId          ≈  partición de Kafka
        # SequenceNumber   ≈  offset de Kafka
        if (i + 1) % 50 == 0:
            print(
                f"[{i + 1}/{N_PEDIDOS}] shard={response['ShardId']} "
                f"secuencia={response['SequenceNumber'][:20]}..."
            )

        time.sleep(0.1)  # ~10 pedidos/segundo

    print(f"✅ {N_PEDIDOS} pedidos enviados a Kinesis")
    print("\nVerifica en AWS Console: Kinesis → "
          f"{STREAM} → Monitoring → PutRecords success rate (debería ser 100%)")


if __name__ == "__main__":
    main()
