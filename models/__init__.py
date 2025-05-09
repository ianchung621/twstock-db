from models.adjusted_price import AdjustedPrice
from models.broker_info import BrokerInfo
from models.broker_transaction import BrokerTransaction
from models.contract_info import ContractInfo
from models.future_large_trader import FutureLargeTrader
from models.index_price import IndexPrice
from models.stock_cap_reduction import StockCapReduction
from models.stock_dividend import StockDividend
from models.stock_info import StockInfo
from models.stock_price import StockPrice
from models.stock_split import StockSplit
from models.stock_revenue import StockRevenue
from models.stock_ii import StockII
from models.base import Base

from typing import Type
from sqlalchemy.orm import DeclarativeBase

def get_model(model_name: str) -> Type[DeclarativeBase]:
    model_cls = globals().get(model_name)
    if model_cls is None:
        raise AttributeError(f"Model '{model_name}' not found in models/__init__.py")
    return model_cls