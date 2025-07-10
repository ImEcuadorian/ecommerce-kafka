import json, sqlite3, threading, time
from pathlib import Path
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field

BASE = Path(__file__).parent
DB_PATH = BASE / "inventory.db"
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "order-confirmed"

app = FastAPI(title="Inventory Service")

class StockUpdate(BaseModel):
    # alias="productId" permite recibir {"productId": "..."}
    product_id: int = Field(alias="productId")
    quantity:    int

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

db = get_db()

@app.get("/inventory/{product_id}")
def read_stock(product_id: int):
    c = db.cursor()
    c.execute("SELECT id, name, stock FROM products WHERE id = ?", (product_id,))
    row = c.fetchone()
    if not row:
        raise HTTPException(404, "Producto no encontrado")
    return {"id": row["id"], "name": row["name"], "stock": row["stock"]}

@app.post("/inventory/update")
def update_stock(u: StockUpdate):
    c = db.cursor()
    c.execute("SELECT stock FROM products WHERE id = ?", (u.product_id,))
    row = c.fetchone()
    if not row:
        raise HTTPException(404, "Producto no existe")
    new_stock = row["stock"] - u.quantity
    if new_stock < 0:
        raise HTTPException(400, "Stock insuficiente")
    c.execute("UPDATE products SET stock = ? WHERE id = ?", (new_stock, u.product_id))
    db.commit()
    return {"product_id": u.product_id, "new_stock": new_stock}

def kafka_listener():
    # retry loop para esperar a Kafka
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset="earliest",
                group_id="inventory-group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("▶️ Connected to Kafka, listening on", TOPIC)
            break
        except NoBrokersAvailable:
            print("⚠️ Kafka no disponible, reintentando en 5s…")
            time.sleep(5)

    for msg in consumer:
        event = msg.value
        if event.get("type") != "order_created":
            continue

        order = event.get("data", {})
        items = order.get("items", [])
        if not items:
            print(f"ℹ️ Pedido {order.get('order_id')} sin items, saltando")
            continue

        for item in items:
            try:
                # ahora StockUpdate(**item) funciona con {"productId": "..."}
                update_stock(StockUpdate(**item))
                print(f"✔️ Reduced stock: {item}")
            except Exception as e:
                print("⚠️ Error actualizando stock para", item, ":", e)

@app.on_event("startup")
def start_kafka_thread():
    threading.Thread(target=kafka_listener, daemon=True).start()
