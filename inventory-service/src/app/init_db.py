import os
import psycopg2
from psycopg2 import sql

initial_products = [
    {"id": 1, "name": "Camiseta",   "stock": 100},
    {"id": 2, "name": "Pantalones", "stock": 50},
    {"id": 3, "name": "Gorra",      "stock": 75},
]

def init():
    DATABASE_URL = "postgresql://postgres:root@db-inventory-service:5432/inventory_db"
    if not DATABASE_URL:
        raise RuntimeError("❌ Define DATABASE_URL")

    # Conexión y cursor
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Crea la tabla si no existe
    cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                                                        id    INTEGER PRIMARY KEY,
                                                        name  TEXT    NOT NULL,
                                                        stock INTEGER NOT NULL
                );
                """)
    # Limpia la tabla
    cur.execute("TRUNCATE TABLE products;")

    # Inserta los datos iniciales
    for p in initial_products:
        cur.execute(
            "INSERT INTO products (id, name, stock) VALUES (%s, %s, %s);",
            (p["id"], p["name"], p["stock"])
        )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ DB inicializada en PostgreSQL")

if __name__ == "__main__":
    init()
