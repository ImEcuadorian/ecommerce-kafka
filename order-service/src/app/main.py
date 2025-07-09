# app/main.py

import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

from src.app.db import init_db
from src.app.routers.orders import router as orders_router


@asynccontextmanager
async def lifespan():
    init_db()
    yield

app = FastAPI(
    title="Order Service",
)

# Registramos el router
app.include_router(orders_router, prefix="/api/orders", tags=["orders"])

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
