from sqlalchemy import Boolean, Column, ForeignKey, Integer, String,Float
from sqlalchemy.orm import relationship

from database import Base


class raw_data(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)

    items = relationship("Item", back_populates="owner")


class Volume(Base):
    __tablename__ = "Volume"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    timestamp = Column(String)
    datetime = Column(String)
    highest_buy_bid = Column(Float)
    lowest_sell_bid = Column(Float)
    last_traded_price = Column(Float)
    yes_price = Column(Float)
    inr_price = Column(Float)
