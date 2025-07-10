from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from src.app.db             import SessionLocal
from src.app.models         import OrderCreate, Order as OrderSchema, OrderORM
from src.app.kafka_producer import send_order_event

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/", response_model=OrderSchema, status_code=status.HTTP_201_CREATED)
def create_order(
        order: OrderCreate,
        db: Session = Depends(get_db),
):
    new = OrderORM(
        customer_name=order.customer_name,
        customer_email=order.customer_email,
        items=[item.model_dump() for item in order.items],
        total=order.total,
    )
    db.add(new)
    db.commit()
    db.refresh(new)

    send_order_event({
        "id": new.id,
        "order_id": new.id,                     # usa siempre order_id
        "customer_email": new.customer_email,   # <<< aquí
        "items": new.items,
        "total": new.total,
        "created_at": new.created_at.isoformat(),
    })

    return new

@router.get("/", response_model=List[OrderSchema])
def list_orders(db: Session = Depends(get_db)):
    orders = db.query(OrderORM).order_by(OrderORM.created_at.desc()).all()
    return orders

@router.get("/{order_id}", response_model=OrderSchema)
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(OrderORM).get(order_id)
    if not order:
        raise HTTPException(404, "Order not found")
    return order
