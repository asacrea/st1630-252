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
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "pedidos-ventas"
N_PEDIDOS = 1000

# ═══════════════════════════════════════════════════════════════
# TODO 1.1 · Configuración del KafkaProducer
# ═══════════════════════════════════════════════════════════════
# Por qué bootstrap_servers solo necesita 1 broker (aunque el clúster
# tuviera 10): el cliente le manda un MetadataRequest a ESE broker, y
# el broker responde con el mapa completo del clúster -- qué broker es
# líder de cada partición. bootstrap_servers es solo la "puerta de
# entrada" para pedir ese mapa, no la lista completa de brokers.
#
# key_serializer / value_serializer: Kafka mueve bytes, no objetos
# Python -- todo lo que le mandes tiene que convertirse a bytes antes
# de salir por la red. json.dumps(...).encode("utf-8") convierte un
# dict a un string JSON y luego a bytes; para la key (un string simple
# como el nombre de la región) basta con .encode("utf-8") directo.
#
# TODO: crea el KafkaProducer con:
#   - bootstrap_servers=[KAFKA_BOOTSTRAP]
#   - key_serializer: función que reciba un string y devuelva bytes
#     (o None si la key es None -- ¿por qué podría ser None? repasa
#     la Parte 0, Pregunta 5)
#   - value_serializer: función que reciba un dict, lo pase por
#     json.dumps() y lo codifique a bytes
#   - acks='all': la garantía de durabilidad más alta que Kafka ofrece
#     -- el líder espera que TODAS las réplicas ISR confirmen antes de
#     responder al producer. En este lab (1 solo broker, sin réplicas
#     reales) acks='all' se comporta igual que acks=1 en la práctica,
#     pero es la configuración que usarías en un clúster real con
#     factor de replicación > 1 y la que corresponde a la garantía
#     at-least-once que vas a construir del lado del consumidor.
#   - linger_ms=10, batch_size=16384: agrupa mensajes en lotes
#     pequeños antes de enviarlos -- mejora throughput a costa de
#     latencia mínima (10ms). No es el foco pedagógico de este lab,
#     pero es buena práctica dejarlo configurado.
producer = None  # TODO: reemplaza por tu KafkaProducer(...)

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
# Decisión de diseño: usamos key=region, NO key=pedido_id.
#
# Con key=region: hash(region) % N_particiones siempre da la misma
# partición para la misma región -- eso GARANTIZA orden dentro de cada
# región (los pedidos de Bogotá se leen en el mismo orden en que se
# escribieron), a costa de balanceo: si Bogotá es el 40% del tráfico,
# su partición recibe el 40% de la carga (hot partition).
#
# Con key=pedido_id (la alternativa): cada pedido tiene una key única,
# así que se reparten mucho más parejo entre particiones (mejor
# balanceo), pero se pierde cualquier noción de "orden por región" --
# dos pedidos de la misma región podrían terminar en particiones
# distintas y leerse en cualquier orden relativo.
#
# TODO: completa el envío síncrono de cada pedido:
#   1. future = producer.send(TOPIC, key=<algo>, value=pedido)
#      -- ¿qué campo del dict `pedido` va como key? (pista: el título
#      de esta sección ya te lo dice)
#   2. metadata = future.get(timeout=10)  # bloquea hasta la confirmación
#   3. usa metadata.partition y metadata.offset para el log de abajo
def enviar_pedido(pedido: dict):
    """Envía un pedido y devuelve (partition, offset) para logging."""
    # TODO: tu código aquí (producer.send + future.get)
    raise NotImplementedError("TODO 1.3: implementa el envío síncrono")


def main():
    conteo_region_particion = defaultdict(lambda: defaultdict(int))

    print(f"Publicando {N_PEDIDOS} pedidos en '{TOPIC}' (bootstrap: {KAFKA_BOOTSTRAP})...")

    for i in range(N_PEDIDOS):
        pedido = generar_pedido()
        partition, offset = enviar_pedido(pedido)

        # TODO 1.4 · Logging: imprime cada 100 mensajes el offset y la
        # partición para no inundar la terminal, pero SIEMPRE acumula
        # el conteo región -> partición (lo necesitas para el resumen
        # final y para responder la Pregunta 2 de kafka_design.md).
        # TODO: acumula conteo_region_particion[pedido["region"]][partition] += 1
        if (i + 1) % 100 == 0:
            print(f"  [{i + 1}/{N_PEDIDOS}] región={pedido['region']:<12} "
                  f"partición={partition} offset={offset}")

    producer.flush()

    # ── Resumen final: región -> partición -> cantidad de mensajes ──
    # Esta tabla es la evidencia que necesitas para responder si Bogotá
    # (40% del tráfico) realmente concentra la carga en una sola
    # partición -- ver Pregunta 2 de kafka_design.md.
    print("\n=== Resumen: región -> partición -> mensajes ===")
    for region in REGIONES:
        particiones = conteo_region_particion.get(region, {})
        detalle = ", ".join(f"P{p}={c}" for p, c in sorted(particiones.items()))
        print(f"  {region:<14} {detalle}")

    producer.close()


if __name__ == "__main__":
    main()
