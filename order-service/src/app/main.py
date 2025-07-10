# app/main.py

import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

from src.app.db import init_db
from src.app.routers.orders import router as orders_router

app = FastAPI(
    title="Order Service",
)

@app.on_event("startup")
def on_startup():
    init_db()

# Registramos el router
app.include_router(orders_router, prefix="/api/orders", tags=["orders"])

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
