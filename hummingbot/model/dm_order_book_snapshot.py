#!/usr/bin/env python
import numpy
from typing import (
    Dict,
    Any
)
from sqlalchemy import (
    Column,
    String,
    BigInteger,
    Integer,
    Float,
    SmallInteger
)

from . import HummingbotBase


class OrderBookSnapshot(HummingbotBase):
    __tablename__ = "OrderBookSnapshot"
    # __table_args__ = (Index("o_config_timestamp_index",
    #                         "config_file_path", "creation_timestamp"),
    #                   Index("o_market_trading_pair_timestamp_index",
    #                         "market", "symbol", "creation_timestamp"),
    #                   Index("o_market_base_asset_timestamp_index",
    #                         "market", "base_asset", "creation_timestamp"),
    #                   Index("o_market_quote_asset_timestamp_index",
    #                         "market", "quote_asset", "creation_timestamp"))

    id = Column(Integer, primary_key=True)
    trading_pair = Column(String(20), nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    type = Column(String(50), nullable=False)
    exchange = Column(String(255), nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    update_id = Column(String(255), nullable=False)
    is_bid = Column(SmallInteger, nullable=False)

    def __repr__(self) -> str:
        return f"OrderBookSnapshot(id={self.id}, trading_pair='{self.trading_pair}', timestamp='{self.timestamp}', " \
               f"exchange='{self.exchange}', price='{self.price}', amount='{self.amount}', " \
               f"update_id='{self.update_id}', is_bid={self.is_bid}, type={self.type}) "

    @staticmethod
    def to_bounty_api_json(orderBookSnapshot: "OrderBookSnapshot") -> Dict[str, Any]:
        return {
            "orderBookSnapshot_id": orderBookSnapshot.id,
            "trading_pair": orderBookSnapshot.trading_pair,
            "timestamp": orderBookSnapshot.timestamp,
            "exchange": orderBookSnapshot.exchange,
            "price": numpy.format_float_positional(orderBookSnapshot.price),
            "amount": numpy.format_float_positional(orderBookSnapshot.amount),
            "update_id": orderBookSnapshot.update_id,
            "is_bid": orderBookSnapshot.is_bid,
            "type": orderBookSnapshot.type
        }
