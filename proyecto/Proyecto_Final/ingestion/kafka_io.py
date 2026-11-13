import os


def bootstrap():
    return os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


class KafkaSender:
    def __init__(self):
        from confluent_kafka import Producer
        self.producer = Producer({'bootstrap.servers': bootstrap(),
                                  'enable.idempotence': True, 'acks': 'all',
                                  'delivery.timeout.ms': 10000, 'request.timeout.ms': 5000,
                                  'retry.backoff.ms': 250, 'client.id': 'shipment-simulator'})

    def __call__(self, topic, key, payload):
        result = []
        self.producer.produce(topic, key=key.encode(), value=payload.encode(),
                              on_delivery=lambda error, msg: result.append(error))
        remaining = self.producer.flush(12)
        if remaining or not result or result[0] is not None:
            raise RuntimeError(f'Kafka no confirmó el evento: {result}')


def create_topics():
    from confluent_kafka import KafkaException, KafkaError
    from confluent_kafka.admin import AdminClient, NewTopic
    from .events import TOPICS
    admin = AdminClient({'bootstrap.servers': bootstrap()})
    futures = admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1,
                                         config={'retention.ms': '604800000'}) for t in TOPICS])
    for topic, future in futures.items():
        try:
            future.result(timeout=30)
            print(f'Creado: {topic}')
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            print(f'Ya existe: {topic}')


def consume(limit, seconds):
    import json
    import time
    from uuid import uuid4
    from confluent_kafka import Consumer
    from .events import validate
    consumer = Consumer({'bootstrap.servers': bootstrap(), 'group.id': f'verify-{uuid4()}',
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    consumer.subscribe(['shipment_events', 'vehicle_gps'])
    deadline, count = time.monotonic() + seconds, 0
    try:
        while count < limit and time.monotonic() < deadline:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())
            event = json.loads(msg.value())
            validate(event)
            print(json.dumps({'topic': msg.topic(), 'partition': msg.partition(),
                              'offset': msg.offset(), 'key': msg.key().decode(), 'event': event}, ensure_ascii=False))
            count += 1
    finally:
        consumer.close()
    if count < limit:
        raise RuntimeError(f'Se esperaban {limit} eventos; recibidos {count}')
