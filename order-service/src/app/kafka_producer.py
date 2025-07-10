import os
import json
import time
import socket
from confluent_kafka import Producer, KafkaException

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC  = "order-confirmed"

def wait_for_broker(broker, interval=5):
    host, port = broker.split(":")
    port = int(port)
    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"✅ Broker {broker} reachable")
                return
        except OSError:
            print(f"⚠️ Broker {broker} no disponible, reintentando en {interval}s…")
            time.sleep(interval)

def create_producer():
    wait_for_broker(BROKER)
    while True:
        try:
            p = Producer({'bootstrap.servers': BROKER})
            # forzar metadata fetch para validar conexión
            p.list_topics(timeout=10)
            print("🎉 Producer conectado a Kafka")
            return p
        except KafkaException as e:
            print(f"⚠️ No se pudo conectar el producer: {e}. Reintentando en 5s…")
            time.sleep(5)

producer = create_producer()

def send_order_event(order_data: dict):
    event = {
        "type": "order_created",
        "data": order_data
    }
    while True:
        try:
            producer.produce(TOPIC, json.dumps(event).encode("utf-8"))
            producer.flush(10)  # hasta 10s de espera
            print(f"✅ Evento enviado: {order_data}")
            break
        except KafkaException as e:
            print(f"⚠️ Error enviando evento: {e}. Reintentando en 5s…")
            time.sleep(5)
