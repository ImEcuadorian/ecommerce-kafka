import os
import json
from confluent_kafka import Producer

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = Producer({
    'bootstrap.servers': BROKER
})

def send_order_event(order_data: dict):
    event = {
        "type": "order_created",
        "data": order_data
    }
    producer.produce("orders", json.dumps(event).encode("utf-8"))
    producer.flush()
