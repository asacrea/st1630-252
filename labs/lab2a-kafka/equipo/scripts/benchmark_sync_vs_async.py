"""benchmark_sync_vs_async.py — Lab 2a, sección 1.5 (opcional) — equipo

Mide cuánto tarda publicar los mismos 1.000 pedidos en dos modos:

  SÍNCRONO  — future.get(timeout=10) después de cada send(). El productor
              se bloquea hasta que el líder confirma la escritura, así que
              hay un round-trip completo por mensaje. Es lo que hace
              productor_kafka.py (TODO 1.3).

  ASÍNCRONO — future.add_callback(...) y un único producer.flush() al
              final. El send() encola y devuelve de inmediato; el
              conteo región->partición se acumula en el callback, cuando
              llega el ack. Es la variante que propone la sección 1.5.

La comparación no es un capricho de microbenchmark: es la que justifica el
cambio (a) de la Pregunta 5 de kafka_design.md. En modo síncrono,
linger_ms=10 y batch_size=16384 están configurados pero NO rinden, porque
el .get() corta cada lote antes de que alcance a llenarse.

Usa un topic aparte (pedidos-ventas-benchmark) para no contaminar los
conteos de Bronze del entregable principal.

Uso:
    python3 benchmark_sync_vs_async.py
"""

import os
import time
from collections import defaultdict

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Se reutiliza el generador de pedidos del entregable principal para que
# ambos modos publiquen exactamente la misma clase de datos.
from productor_kafka import generar_pedido, REGIONES  # noqa: E402

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "pedidos-ventas-benchmark"
N_PEDIDOS = 1000


def crear_topic():
    """Crea el topic de benchmark (4 particiones, igual que el del lab)."""
    admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BOOTSTRAP])
    try:
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=4, replication_factor=1)])
        print(f"Topic '{TOPIC}' creado (4 particiones).")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC}' ya existía.")
    finally:
        admin.close()


def nuevo_producer():
    """Mismo productor del entregable: acks='all', linger_ms=10, batch_size=16384."""
    import json

    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=10,
        batch_size=16384,
    )


def enviar_sincrono(pedidos):
    """Un round-trip por mensaje: send() + get() antes del siguiente."""
    producer = nuevo_producer()
    conteo = defaultdict(lambda: defaultdict(int))

    inicio = time.time()
    for pedido in pedidos:
        future = producer.send(TOPIC, key=pedido["region"], value=pedido)
        metadata = future.get(timeout=10)          # <- el bloqueo
        conteo[pedido["region"]][metadata.partition] += 1
    producer.flush()
    duracion = time.time() - inicio

    producer.close()
    return duracion, conteo


def enviar_asincrono(pedidos):
    """send() encola y sigue; el conteo se acumula en el callback del ack."""
    producer = nuevo_producer()
    conteo = defaultdict(lambda: defaultdict(int))

    def hacer_callback(region):
        # El callback recibe el RecordMetadata cuando el broker confirma.
        # Se cierra sobre `region` porque para entonces el loop ya avanzó.
        def al_confirmar(metadata):
            conteo[region][metadata.partition] += 1
        return al_confirmar

    inicio = time.time()
    for pedido in pedidos:
        (producer
         .send(TOPIC, key=pedido["region"], value=pedido)
         .add_callback(hacer_callback(pedido["region"])))
    producer.flush()                                # <- el único bloqueo
    duracion = time.time() - inicio

    producer.close()
    return duracion, conteo


def resumen(conteo):
    return {r: dict(sorted(conteo.get(r, {}).items())) for r in REGIONES if conteo.get(r)}


def main():
    crear_topic()

    # Los MISMOS pedidos en los dos modos, para que la comparación no
    # dependa de la aleatoriedad del generador.
    print(f"Generando {N_PEDIDOS} pedidos...")
    pedidos = [generar_pedido() for _ in range(N_PEDIDOS)]

    print(f"\n[1/2] Enviando {N_PEDIDOS} pedidos en modo SÍNCRONO...")
    t_sync, conteo_sync = enviar_sincrono(pedidos)
    print(f"      {t_sync:.2f} s  ({N_PEDIDOS / t_sync:.1f} msg/s)")

    print(f"\n[2/2] Enviando {N_PEDIDOS} pedidos en modo ASÍNCRONO...")
    t_async, conteo_async = enviar_asincrono(pedidos)
    print(f"      {t_async:.2f} s  ({N_PEDIDOS / t_async:.1f} msg/s)")

    print("\n=== Comparación ===")
    print(f"  Síncrono   : {t_sync:8.2f} s   {N_PEDIDOS / t_sync:8.1f} msg/s")
    print(f"  Asíncrono  : {t_async:8.2f} s   {N_PEDIDOS / t_async:8.1f} msg/s")
    print(f"  Aceleración: {t_sync / t_async:8.2f}x")

    # Verificación de que el modo no cambia el destino de los mensajes: la
    # key sigue decidiendo la partición, sea el envío bloqueante o no.
    iguales = resumen(conteo_sync) == resumen(conteo_async)
    print(f"\n  Misma distribución región->partición en ambos modos: {iguales}")
    print(f"  Distribución: {resumen(conteo_sync)}")


if __name__ == "__main__":
    main()
