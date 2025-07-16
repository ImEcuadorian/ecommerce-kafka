import os
import json
import threading
import time

from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# ——— Configuración básica —————————————————————————————
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DATABASE_URL   = os.getenv("DATABASE_URL", "postgresql://postgres:root@db-inventory-service:5432/inventory_db")

# Definición de topics
TOPIC_CONSUME = "order-confirmed"
TOPIC_PRODUCE = "order-validated"

app = FastAPI(title="Inventory Service")

# ——— Configuración del pool de conexiones Postgres ———————————
db_pool: pool.ThreadedConnectionPool | None = None

@app.on_event("startup")
def startup():
    global db_pool
    # Inicializa pool: min 1, max 10 conexiones
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=100,
        dsn=DATABASE_URL,
        cursor_factory=RealDictCursor
    )
    # Asegura que la tabla exista
    conn = db_pool.getconn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS products (
                                                                id    INTEGER PRIMARY KEY,
                                                                name  TEXT    NOT NULL,
                                                                stock INTEGER NOT NULL
                        );
                        """)
    db_pool.putconn(conn)

@app.on_event("shutdown")
def shutdown():
    if db_pool:
        db_pool.closeall()

# ——— Helper para obtener conexión ————————————————————————
def get_db_conn():
    if not db_pool:
        raise RuntimeError("DB pool no inicializado")
    return db_pool.getconn()

def release_db_conn(conn):
    if db_pool:
        db_pool.putconn(conn)

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

# ——— Endpoints HTTP ————————————————————————————————————
@app.get("/inventory/{product_id}")
def read_stock(product_id: int):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, stock FROM products WHERE id = %s;",
                (product_id,)
            )
            product = cur.fetchone()
        if not product:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        return product
    finally:
        release_db_conn(conn)

@app.post("/inventory/update")
def update_stock(u: StockUpdate):
    conn = get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT stock FROM products WHERE id = %s;",
                    (u.product_id,)
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Producto no existe")
                new_stock = row["stock"] - u.quantity
                if new_stock < 0:
                    raise HTTPException(status_code=400, detail="Stock insuficiente")
                cur.execute(
                    "UPDATE products SET stock = %s WHERE id = %s;",
                    (new_stock, u.product_id)
                )
        return {"product_id": u.product_id, "new_stock": new_stock}
    finally:
        release_db_conn(conn)

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
                # Reutilizamos el endpoint interno
                update_stock(StockUpdate(**item))
                print(f"✔️ Reduced stock for {item}")
            except HTTPException as e:
                errores = True
                print(f"⚠️ Stock error for {item}: {e.detail}")
            except Exception as e:
                errores = True
                print(f"⚠️ Unexpected error for {item}: {e}")

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
