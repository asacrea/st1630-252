"""productor_kafka.py — Lab 2a (ST1630-2026-2, S6-S7)

Genera 1.000 pedidos sintéticos y los publica en el topic
"pedidos-ventas". Este script tiene bloques marcados con # TODO --
ese es tu trabajo. Todo lo demás (el generador de datos, la config de
conexión) ya está resuelto para que te concentres en las decisiones
que sí importan esta semana: qué key usar y qué nivel de acks pedir.

Prerequisito: el topic "pedidos-ventas" debe existir ANTES de correr
esto (Parte 0, Pregunta 1 del lab -- créalo a mano con kafka-topics).
KAFKA_AUTO_CREATE_TOPICS_ENABLE=false en el docker-compose, así que si
el topic no existe, vas a ver un error explícito en vez de que Kafka
te lo cree solo.

Uso:
    python3 productor_kafka.py

Qué puedes delegar: boilerplate de kafka-python si te trabas en la
sintaxis. Qué NO puedes delegar: decidir la key y justificarla en
kafka_design.md -- ver ../README.md, "Bitácora de delegación".
"""

import json
import os
import random
import uuid
from collections import defaultdict
from datetime import date, timedelta

from kafka import KafkaProducer

# ─────────────────────────────────────────────────────────────
# Configuración -- funciona en local sin cambios; KAFKA_BOOTSTRAP
# permite apuntar a otro clúster (p. ej. en producción) sin tocar código.
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TOPIC = "pedidos-ventas"
N_PEDIDOS = 1000

# ═══════════════════════════════════════════════════════════════
# TODO 1.1 · Configuración del KafkaProducer
# ═══════════════════════════════════════════════════════════════
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    linger_ms=10,
    batch_size=16384,
)

# ─────────────────────────────────────────────────────────────
# Generador de pedidos sintéticos (dado -- no hay decisión de diseño
# aquí, ya está resuelto)
# ─────────────────────────────────────────────────────────────
REGIONES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Bucaramanga", "Otro"]
PESOS_REGION = [0.40, 0.20, 0.15, 0.10, 0.08, 0.07]

CATALOGO = {
    "Electrónica": ["Audífonos", "Cargador", "Mouse", "Teclado", "Parlante Bluetooth"],
    "Ropa": ["Camiseta", "Pantalón", "Chaqueta", "Zapatos", "Gorra"],
    "Alimentos": ["Café molido", "Panela", "Chocolate", "Arroz", "Aceite"],
    "Hogar": ["Licuadora", "Cafetera", "Aspiradora", "Lámpara", "Ventilador"],
    "Deportes": ["Balón", "Bicicleta", "Mancuernas", "Tenis running", "Maleta deportiva"],
    "Belleza": ["Shampoo", "Crema facial", "Perfume", "Maquillaje", "Protector solar"],
}
CATEGORIAS = list(CATALOGO.keys())

CANALES = ["app_movil", "web", "tienda_fisica", "telefono"]
METODOS_PAGO = ["tarjeta_credito", "tarjeta_debito", "efectivo", "nequi", "daviplata", "transferencia"]

FECHA_INICIO = date(2026, 1, 1)
RANGO_DIAS = 365


def generar_pedido() -> dict:
    region = random.choices(REGIONES, weights=PESOS_REGION, k=1)[0]
    categoria = random.choice(CATEGORIAS)
    producto = random.choice(CATALOGO[categoria])
    cantidad = random.randint(1, 8)
    precio_unit = round(random.uniform(8_000, 3_500_000), -2)
    fecha = FECHA_INICIO + timedelta(days=random.randint(0, RANGO_DIAS - 1))

    return {
        "pedido_id": str(uuid.uuid4()),
        "fecha": fecha.isoformat(),
        "region": region,
        "categoria": categoria,
        "producto": producto,
        "cantidad": cantidad,
        "precio_unit": precio_unit,
        "total": round(cantidad * precio_unit, 2),
        "canal": random.choice(CANALES),
        "metodo_pago": random.choice(METODOS_PAGO),
        "devuelto": random.random() < 0.07,
    }


# ═══════════════════════════════════════════════════════════════
# TODO 1.3 · Envío con key=region
# ═══════════════════════════════════════════════════════════════
def enviar_pedido(pedido: dict):
    """Envía un pedido y devuelve (partition, offset) para logging."""
    future = producer.send(TOPIC, key=pedido["region"], value=pedido)
    metadata = future.get(timeout=10)
    return metadata.partition, metadata.offset


def main():
    conteo_region_particion = defaultdict(lambda: defaultdict(int))

    print(f"Publicando {N_PEDIDOS} pedidos en '{TOPIC}' (bootstrap: {KAFKA_BOOTSTRAP})...")

    for i in range(N_PEDIDOS):
        pedido = generar_pedido()
        partition, offset = enviar_pedido(pedido)

        conteo_region_particion[pedido["region"]][partition] += 1
        if (i + 1) % 100 == 0:
            print(f"  [{i + 1}/{N_PEDIDOS}] región={pedido['region']:<12} "
                  f"partición={partition} offset={offset}")

    producer.flush()

    # ── Resumen final: región -> partición -> cantidad de mensajes ──
    print("\n=== Resumen: región -> partición -> mensajes ===")
    for region in REGIONES:
        particiones = conteo_region_particion.get(region, {})
        detalle = ", ".join(f"P{p}={c}" for p, c in sorted(particiones.items()))
        print(f"  {region:<14} {detalle}")

    producer.close()


if __name__ == "__main__":
    main()
