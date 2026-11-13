
import json
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from jsonschema import Draft202012Validator, FormatChecker


KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "operational_updates"

MONGO_URI = os.environ["MONGO_URI"]
DATABASE = "sid_logistica"
COLLECTION = "shipments_current"

with open("schemas/operational.schema.json", encoding="utf-8") as file:
    schema = json.load(file)

validator = Draft202012Validator(
    schema,
    format_checker=FormatChecker()
)


def parse_event(raw_message):
    event = json.loads(raw_message.decode("utf-8"))
    validator.validate(event)

    timestamp = datetime.fromisoformat(
        event["event_time"].replace("Z", "+00:00")
    )

    if timestamp.tzinfo is None:
        raise ValueError("event_time debe incluir zona horaria")

    event["event_time"] = timestamp.astimezone(timezone.utc)
    return event


def save_event(collection, event):
    shipment_id = event["shipment_id"]
    event_time = event["event_time"]
    event_id = event["event_id"]

    # Solo aceptar eventos más recientes que el estado almacenado.
    filter_query = {
        "shipment_id": shipment_id,
        "$or": [
            {"event_time": {"$lt": event_time}},
            {
                "event_time": event_time,
                "event_id": {"$lt": event_id}
            },
            {"event_time": {"$exists": False}}
        ]
    }

    try:
        result = collection.update_one(
            filter_query,
            {"$set": event},
            upsert=True
        )
        return result.modified_count > 0 or result.upserted_id is not None
    except DuplicateKeyError:
        # Un evento antiguo o repetido no debe sobrescribir
        # el estado actual de la guía.
        return False


def main():
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo.admin.command("ping")

    collection = mongo[DATABASE][COLLECTION]
    collection.create_index("shipment_id", unique=True)
    collection.create_index("active_alert")
    collection.create_index("risk_level")

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "sid-mongodb-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([KAFKA_TOPIC])

    print("Conectado a MongoDB")
    print(f"Escuchando Kafka: {KAFKA_TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                event = parse_event(message.value())
            except (ValueError, KeyError, json.JSONDecodeError) as error:
                print(f"Evento inválido: {error}")
                # Pendiente: cuarentena para mensajes inválidos.
                # No confirmar para evitar pérdida silenciosa.
                raise
            except Exception as error:
                print(f"Error de validación: {error}")
                raise

            updated = save_event(collection, event)
            consumer.commit(message=message, asynchronous=False)

            status = "Actualizado" if updated else "Duplicado/antiguo"
            print(f"{status}: {event['shipment_id']}")

    except KeyboardInterrupt:
        print("Consumidor detenido")
    finally:
        consumer.close()
        mongo.close()


if __name__ == "__main__":
    main()
