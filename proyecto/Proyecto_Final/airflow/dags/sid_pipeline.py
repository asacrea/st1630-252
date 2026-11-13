
import os
import socket
from datetime import datetime, timezone

from airflow.sdk import dag, task
from pymongo import MongoClient


@dag(
    dag_id="sid_supervision",
    description="Supervision del pipeline logistico SID",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["SID", "logistica", "monitoreo"],
)
def sid_supervision():

    @task
    def verificar_kafka():
        broker = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:29092"
        )

        host, port = broker.split(":")

        with socket.create_connection(
            (host, int(port)), timeout=10
        ):
            print("Kafka accesible:", broker)

        return True

    @task
    def verificar_mongodb():
        with MongoClient(
            host=os.getenv("MONGO_HOST", "mongodb"),
            port=int(os.getenv("MONGO_PORT", "27017")),
            username=os.environ["MONGO_USER"],
            password=os.environ["MONGO_PASSWORD"],
            authSource="admin",
            serverSelectionTimeoutMS=5000,
        ) as client:

            client.admin.command("ping")
            print("MongoDB conectado correctamente")

        return True

    @task
    def supervisar_envios():
        with MongoClient(
            host=os.getenv("MONGO_HOST", "mongodb"),
            port=int(os.getenv("MONGO_PORT", "27017")),
            username=os.environ["MONGO_USER"],
            password=os.environ["MONGO_PASSWORD"],
            authSource="admin",
            serverSelectionTimeoutMS=5000,
        ) as client:

            collection = client[
                "sid_logistica"
            ]["shipments_current"]

            total = collection.count_documents({})
            alertas = collection.count_documents({
                "active_alert": True
            })
            riesgo_alto = collection.count_documents({
                "risk_level": "high"
            })

            print("Total de envios:", total)
            print("Alertas activas:", alertas)
            print("Envios de alto riesgo:", riesgo_alto)

            return {
                "total": total,
                "alertas": alertas,
                "riesgo_alto": riesgo_alto,
            }

    kafka = verificar_kafka()
    mongo = verificar_mongodb()
    monitoreo = supervisar_envios()

    [kafka, mongo] >> monitoreo


sid_supervision()
