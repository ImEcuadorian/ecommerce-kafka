import json
import sqlite3
import threading
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field

# ——— Configuración básica —————————————————————————————
BASE = Path(__file__).parent
DB_PATH = BASE / "inventory.db"
KAFKA_BOOTSTRAP = "kafka:9092"

# Definición de topics
TOPIC_CONSUME = "order-confirmed"
TOPIC_PRODUCE = "order-validated"

app = FastAPI(title="Inventory Service")

# ——— Producer de Kafka (lazy init) ———————————————————————
producer: KafkaProducer | None = None
def get_producer() -> KafkaProducer:
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    return producer

# ——— Modelo de datos para actualización de stock ———————————
class StockUpdate(BaseModel):
    product_id: int = Field(alias="productId")
    quantity:    int

# ——— Conexión a SQLite ———————————————————————————————
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

db = get_db()

# ——— Endpoints HTTP ————————————————————————————————————
@app.get("/inventory/{product_id}")
def read_stock(product_id: int):
    c = db.cursor()
    c.execute("SELECT id, name, stock FROM products WHERE id = ?", (product_id,))
    row = c.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Producto no encontrado")
    return {"id": row["id"], "name": row["name"], "stock": row["stock"]}

@app.post("/inventory/update")
def update_stock(u: StockUpdate):
    c = db.cursor()
    c.execute("SELECT stock FROM products WHERE id = ?", (u.product_id,))
    row = c.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Producto no existe")
    new_stock = row["stock"] - u.quantity
    if new_stock < 0:
        raise HTTPException(status_code=400, detail="Stock insuficiente")
    c.execute("UPDATE products SET stock = ? WHERE id = ?", (new_stock, u.product_id))
    db.commit()
    return {"product_id": u.product_id, "new_stock": new_stock}

# ——— Listener de Kafka en background —————————————————————
def kafka_listener():
    # 1) Espera a que Kafka esté disponible
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_CONSUME,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset="earliest",
                group_id="inventory-group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print(f"▶️ Connected to Kafka, listening on '{TOPIC_CONSUME}'")
            break
        except NoBrokersAvailable:
            print("⚠️ Kafka no disponible, reintentando en 5s…")
            time.sleep(5)

    # 2) Procesa cada mensaje
    for msg in consumer:
        event = msg.value
        if event.get("type") != "order_created":
            continue

        order = event.get("data", {})
        order_id       = order.get("order_id")
        customer_email = order.get("customer_email")
        items          = order.get("items", [])
        if not items:
            print(f"ℹ️ Pedido {order_id} sin items, saltando")
            continue

        errores = False
        for item in items:
            try:
                update_stock(StockUpdate(**item))
                print(f"✔️ Reduced stock for {item}")
            except HTTPException as e:
                errores = True
                print(f"⚠️ Stock error for {item}: {e.detail}")
            except Exception as e:
                errores = True
                print(f"⚠️ Unexpected error for {item}: {e}")

        # 3) Si todo salió bien, enviamos evento validado CON email
        if not errores:
            validated_event = {
                "type": "order_validated",
                "data": {
                    "order_id":       order_id,
                    "customer_email": customer_email
                }
            }
            prod = get_producer()
            prod.send(TOPIC_PRODUCE, validated_event)
            prod.flush()
            print(f"✉️ Sent 'order_validated' for order {order_id} to {customer_email}")

# ——— Arranca el listener al iniciar la aplicación ——————————
@app.on_event("startup")
def start_kafka_thread():
    threading.Thread(target=kafka_listener, daemon=True).start()
