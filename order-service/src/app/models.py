from datetime       import datetime
from typing         import List

from sqlalchemy import Column, Integer, Float, Text, DateTime as SaDateTime, String
from sqlalchemy.sql import func
from sqlalchemy.types import JSON
from .db            import Base

from pydantic import BaseModel, EmailStr


class OrderORM(Base):
    __tablename__ = "orders"

    id            = Column(Integer, primary_key=True, index=True)
    customer_name = Column(Text,    nullable=False)
    customer_email = Column(String, nullable=False)
    items         = Column(JSON,    nullable=False)
    total         = Column(Float,   nullable=False)
    created_at    = Column(
        SaDateTime(timezone=True),
        server_default=func.now()
    )


class Item(BaseModel):
    productId: str
    quantity:  int

class OrderCreate(BaseModel):
    customer_name: str
    customer_email: EmailStr
    items:          List[Item]
    total:          float

class Order(OrderCreate):
    id:         int
    created_at: datetime

    model_config = {
        "from_attributes": True
    }
