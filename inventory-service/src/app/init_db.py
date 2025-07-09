import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "inventory.db"

initial_products = [
    {"id": 1, "name": "Camiseta",   "stock": 100},
    {"id": 2, "name": "Pantalones", "stock": 50},
    {"id": 3, "name": "Gorra",      "stock": 75},
]

def init():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id      INTEGER PRIMARY KEY,
            name    TEXT    NOT NULL,
            stock   INTEGER NOT NULL
        );
    """)
    c.execute("DELETE FROM products;")
    for p in initial_products:
        c.execute(
            "INSERT INTO products (id, name, stock) VALUES (?, ?, ?);",
            (p["id"], p["name"], p["stock"])
        )
    conn.commit()
    conn.close()
    print("✅ DB inicializada en", DB_PATH)

if __name__ == "__main__":
    init()
