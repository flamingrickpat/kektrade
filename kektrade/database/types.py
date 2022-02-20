from typing import Dict, List, Any
import enum
import copy
from pathlib import Path
import logging

from sqlalchemy.orm import declarative_base
from sqlalchemy import ForeignKey, ForeignKeyConstraint
from sqlalchemy import Column, Integer, String, Float, Enum, Boolean, DateTime
from sqlalchemy.orm import class_mapper
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session as SessionClass

from kektrade.exchange.history_meta import Versioned
from kektrade.misc import EnumComparable

Base = declarative_base()

class OrderStatus(EnumComparable):
    OPEN = 1
    CLOSED = 2
    CANCELED = 3
    EXPIRED = 4

class OrderType(EnumComparable):
    MARKET = 1
    LIMIT = 2
    STOP_MARKET = 3

class Side(EnumComparable):
    NONE = 0
    BUY = 1
    SELL = 2

class TimeInForce(EnumComparable):
    GTC = 1
    IOC = 2
    FOK = 3
    PO = 4

class ExecutionType(EnumComparable):
    TRADE = 1
    LIQUIDATION = 2
    FUNDING = 3

class TakerMakerType(EnumComparable):
    TAKER = 1
    MAKER = 2

class ReduceExpandType(EnumComparable):
    REDUCE = 1
    EXPAND = 2

class PositionStatus(EnumComparable):
    OPEN = 1
    CLOSED = 2
    LIQUIDATING = 3

def copy_sqla_object(obj, omit_fk=True):
    """
    https://groups.google.com/g/sqlalchemy/c/wb2M_oYkQdY/m/gQ-qKsoEBAAJ
    Given an SQLAlchemy object, creates a new object (FOR WHICH THE OBJECT
    MUST SUPPORT CREATION USING __init__() WITH NO PARAMETERS), and copies
    across all attributes, omitting PKs, FKs (by default), and relationship
    attributes.
    """
    cls = type(obj)
    mapper = class_mapper(cls)
    newobj = cls()  # not: cls.__new__(cls)
    pk_keys = set([c.key for c in mapper.primary_key])
    rel_keys = set([c.key for c in mapper.relationships])
    prohibited = pk_keys | rel_keys
    if omit_fk:
        fk_keys = set([c.key for c in mapper.columns if c.foreign_keys])
        prohibited = prohibited | fk_keys
    for k in [p.key for p in mapper.iterate_properties
              if p.key not in prohibited]:
        try:
            value = getattr(obj, k)
            setattr(newobj, k, value)
        except AttributeError:
            pass
    return newobj

def get_engine(path: Path):
    """
    Create a sqlalchemy engine with the sql database of the path.
    Create the database if it does not exist.
    :param path: path to db
    :return: sql alchemy engine object
    """
    MY_SQL_URL = 'sqlite:///' + str(path)
    engine = create_engine(MY_SQL_URL)
    logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)
    return engine

def get_session(path: Path) -> SessionClass:
    """
    Create a sqlalchemy session with the sql database of the path.
    Create the database if it does not exist.
    :param path: path to db
    :return: sql alchemy session object
    """
    engine = get_engine(path)
    Base.metadata.create_all(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

class Subaccount(Base):
    __tablename__ = "subaccount"

    id = Column(Integer, primary_key=True, autoincrement=True)
    subaccount_id = Column(String)
    strategy = Column(String)
    parameter = Column(String)
    is_optimize = Column(Boolean)
    parent_subaccount = Column(Integer)
    optimize_id = Column(Integer)
    start = Column(DateTime)
    end = Column(DateTime)

class OptimizeConfiguration(Base):
    __tablename__ = "optimization"

    id = Column(Integer, primary_key=True, autoincrement=True)
    subaccount_id = Column(String)
    parameters = Column(String)

class Pair(Base):
    __tablename__ = "pair"

    id = Column(Integer, primary_key=True, autoincrement=True)
    subaccount_id = Column(Integer)
    pair = Column(String)
    timeframe = Column(Integer)
    datasource = Column(String)

class Wallet(Base):
    __tablename__ = "wallet"

    id = Column(Integer, primary_key=True, autoincrement=True)  # PK
    subaccount_id = Column(Integer)
    datetime = Column(DateTime) # ISO8601 representation of the unix time above
    timestamp = Column(Integer) # integer unix time since 1st Jan 1970 in milliseconds
    #current_datetime = Column(DateTime) # updates every tick, for history and plotting
    #current_timestamp = Column(Integer) # updates every tick, for history and plotting
    #symbol = Column(String)  # uppercase string literal of a pair of currencies
    #free = Column(Float) # float, money available for trading
    #used = Column(Float)  # float, money on hold, locked, frozen or pending
    #total = Column(Float)  # float, total balance (free + used)

    deposit = Column(Float) # initial deposit
    account_balance = Column(Float) # account balance = deposit + total rpnl
    margin_balance = Column(Float) # total balance = account balance + upnl
    available_balance = Column(Float) # free balance = marign balance - order_marign - position_margin
    total_rpnl = Column(Float) # total realised profit
    order_margin = Column(Float)
    position_margin = Column(Float)



class Position(Base):
    __tablename__ = "position"

    id = Column(Integer, primary_key=True, autoincrement=True)  # PK

    subaccount_id = Column(Integer)

    position_id = Column(String) # string, position id to reference the position, similar to an order id
    #symbol = Column(String) # uppercase string literal of a pair of currencies
    datetime = Column(DateTime)  # ISO8601 representation of the unix time above
    timestamp = Column(Integer) # integer unix time since 1st Jan 1970 in milliseconds
    #current_datetime = Column(DateTime) # updates every tick, for history and plotting
    #current_timestamp = Column(Integer) # updates every tick, for history and plotting
    isolated = Column(Boolean) # boolean, whether or not the position is isolated, as opposed to cross where margin is added automatically
    hedged = Column(Boolean) # boolean, whether or not the position is hedged, i.e. if trading in the opposite direction will close this position or make a new one
    #side = Column(Enum(Side)) # string, long or short
    contracts = Column(Float) # float, number of contracts bought, aka the amount or size of the position
    price = Column(Float) # float, the average entry price of the position
    #markPrice = Column(Float) # float, a price that is used for funding calculations
    #notional = Column(Float) # float, the number of contracts times the price
    leverage = Column(Float) # float, the leverage of the position, related to how many contracts you can buy with a given amount of collateral

    collateral = Column(Float) # float, the maximum amount of collateral that can be lost, affected by pnl

    initialMargin = Column(Float) # float, the amount of collateral that is locked up in this position in the same currency as the notional
    initialMarginPercentage = Column(Float)  # float, the initialMargin as a percentage of the notional

    maintenanceMargin = Column(Float) # float, the mininum amount of collateral needed to avoid being liquidated in the same currency as the notional
    maintenanceMarginPercentage = Column(Float) # float, the maintenanceMargin as a percentage of the notional

    unrealizedPnl = Column(Float) # float, the difference between the market price and the entry price times the number of contracts, can be negative
    unrealizedPnlPercentage = Column(Float)  # float, ROI of position in percent
    liquidationPrice = Column(Float) # float, the price at which collateral becomes less than maintenanceMargin
    bankruptcyPrice = Column(Float)
    status = Column(Enum(PositionStatus)) # string, can be "open", "closed" or "liquidating"
    #info = Column(String) # json response returned from the exchange as is


class Order(Base):
    __tablename__ = "order"

    id = Column(Integer, primary_key=True, autoincrement=True) # PK
    subaccount_id = Column(Integer)
    order_id = Column(String) # order ID
    client_order_id = Column(String) # user defined order ID
    insert_state = Column(Float) # for backtest analysis, only true if just created
    datetime = Column(DateTime) # ISO8601 datetime of 'timestamp' with milliseconds
    timestamp = Column(Integer) # order placing/opening Unix timestamp in milliseconds
    #current_datetime = Column(DateTime) # updates every tick, for history and plotting
    #current_timestamp = Column(Integer) # updates every tick, for history and plotting
    last_trade_datetime = Column(DateTime)  # Unix timestamp of the most recent trade on this order
    last_trade_timestamp = Column(Integer) # Unix timestamp of the most recent trade on this order
    status = Column(Enum(OrderStatus))
    #symbol = Column(String)
    order_type = Column(Enum(OrderType))
    #time_in_force = Column(Enum(TimeInForce))
    side = Column(Enum(Side))
    hedged = Column(Boolean)  # boolean, whether or not the position is hedged, i.e. if trading in the opposite direction will close this position or make a new one
    hedge_mode = Column(Boolean)
    price = Column(Float) # float price in quote currency (may be empty for market orders)
    #average = Column(Float) # float average filling price
    contracts = Column(Float)  # ordered amount of base currency
    #amount = Column(Float) # ordered amount of base currency
    #filled = Column(Float) # filled amount of base currency
    #remaining = Column(Float) # remaining amount to fill
    #cost = Column(Float) # 'filled' * 'price' (filling price used where available)
    #trades = Column(String) # a list of order trades/executions
    #fee_currency = Column(String) # which currency the fee is (usually quote)
    #fee_cost = Column(Float) # the fee amount in that currency
    fee_rate = Column(Float) # the fee rate (if available)
    reduce_only = Column(Float) # reduce only flag
    post_only = Column(Float) # post only flag
    taker_or_maker = Column(Enum(TakerMakerType))
    #info = Column(String) # the original unparsed order structure as is


class Execution(Base):
    __tablename__ = "execution"

    id = Column(Integer, primary_key=True, autoincrement=True)  # PK
    subaccount_id = Column(Integer)
    execution_id = Column(String)  # string ID
    datetime = Column(DateTime)  # ISO8601 datetime of 'timestamp' with milliseconds
    timestamp = Column(Integer)  # order placing/opening Unix timestamp in milliseconds
    #current_datetime = Column(DateTime) # updates every tick, for history and plotting
    #current_timestamp = Column(Integer) # updates every tick, for history and plotting
    #symbol = Column(String)
    execution_type = Column(Enum(ExecutionType))
    order_id = Column(String)
    taker_or_maker = Column(Enum(TakerMakerType))
    price = Column(Float)  # float price in quote currency
    contracts = Column(Float) # contracts
    #amount = Column(Float)  # amount of base currency
    cost = Column(Float)  #  total realized cost or profit (price * amount)
    #fee_currency = Column(String) # usually base currency for buys, quote currency for sells
    fee_cost = Column(Float) # float
    fee_rate = Column(Float) # the fee rate (if available)
    #info = Column(String)  # the original unparsed order structure as is
    reduce_or_expand = Column(Enum(ReduceExpandType)) # is the execution reducing a position





class Order1():




    def __init__(self):
        # Exchange
        self.id = None
        self.type = None
        self.price = None
        self.contracts = None
        self.status = None
        self.fee = 0
        self.reduce_only = False
        self.post_only = False
        self.oco_order = []

        # Zeitlich
        self.creation_time = None
        self.current_time = None
        self.open_time = None
        self.close_time = None
        self.cancel_time = None

        self.creation_i = None
        self.current_i = None
        self.open_i = None
        self.close_i = None
        self.cancel_i = None

        # Lifetime
        self.duration = 0
        self.max_lifetime = 0

        # Exchange Snapshot-Daten fürs Plotting
        # Werden nur bei Backtets verwendet
        self.deposit = None
        self.account_balance = None
        self.margin_balance = None
        self.available_balance = None
        self.upnl = None
        self.upnlp = None
        self.rpnl = None
        self.position_margin = None
        self.order_margin = None
        self.position = None
        self.average_entry_price = None
        self.order_profit = 0

        # self.tp = None
        # self.sl = None
        # self.tp_order = None
        # self.sl_order = None

    def cancel(self):
        raise Exception("not implemented")
        self.status = OrderStatus.CANCELED

    def set_price(self, price):
        raise Exception("not implemented")
        self.price = price

    # def set_tp(self, tp):
    #    if self.tp_order is not None:
    #        if self.tp_order.status == OrderStatus.ACTIVE:
    #            self.tp_order.price = tp
    #    if tp is None:
    #        if self.tp_order is not None and self.tp_order.status == OrderStatus.ACTIVE:
    #            self.tp_order.cancel()
    #        self.tp_order = None
    #        self.tp = None
    #
    # def set_sl(self, sl):
    #    if self.sl_order is not None:
    #        if self.sl_order.status == OrderStatus.ACTIVE:
    #            self.sl_order.price = sl
    #    if sl is None:
    #        if self.sl_order.status is not None and self.sl_order.status == OrderStatus.ACTIVE:
    #            self.sl_order.cancel()
    #        self.sl_order = None
    #        self.sl = None
    #

    def set_params_bybit(self, order):
        """
                            Created - order accepted by the system but not yet put through matching engine
                            Rejected
                            New - order has placed successfully
                            PartiallyFilled
                            Filled
                            Cancelled
                            PendingCancel - the matching engine has received the cancellation but there is no guarantee that it will be successful
        """

        # {
        #    "order_id": "53c02936-2ba9-4fb8-afbf-9e6d31ee7314",
        #    "order_link_id": "",
        #    "symbol": "BTCUSD",
        #    "side": "Buy",
        #    "order_type": "Market",
        #    "price": "11071",
        #    "qty": 1,
        #    "time_in_force": "ImmediateOrCancel",
        #    "create_type": "CreateByClosing",
        #    "cancel_type": "",
        #    "order_status": "Filled",
        #    "leaves_qty": 0,
        #    "cum_exec_qty": 1,
        #    "cum_exec_value": "0.00009032",
        #    "cum_exec_fee": "0.00000007",
        #    "timestamp": "2020-09-19T15:26:54.290Z",
        #    "take_profit": "0",
        #    "stop_loss": "0",
        #    "trailing_stop": "0",
        #    "last_exec_price": "11071"
        # }

        self.id = order["order_id"]
        self.price = float(order["last_exec_price"])

        status = order["order_status"]
        if status == "Created":
            s = OrderStatus.ACTIVE
        elif status == "New":
            s = OrderStatus.ACTIVE
        elif status == "PartiallyFilled":
            s = OrderStatus.ACTIVE
        elif status == "Rejected":
            s = OrderStatus.CANCELED
        elif status == "Cancelled":
            s = OrderStatus.CANCELED
        elif status == "PendingCancel":
            s = OrderStatus.CANCELED
        elif status == "Filled":
            s = OrderStatus.CLOSED
        else:
            raise Exception("unknown order status")

        now = pydt_to_pdts(datetime.datetime.utcnow())
        if self.status is None:
            self.creation_time = now
            self.status = OrderStatus.NONE  # Damit Vergleich möglich ist!

        if s == OrderStatus.ACTIVE and self.status != OrderStatus.ACTIVE:
            self.open_time = now
        if s == OrderStatus.CLOSED and self.status != OrderStatus.CLOSED:
            self.close_time = now
        if s == OrderStatus.CANCELED and self.status != OrderStatus.CANCELED:
            self.cancel_time = now

        self.status = s

        if order["side"] == "Buy":
            self.contracts = abs(int(order["qty"]))
        if order["side"] == "Sell":
            self.contracts = -1 * abs(int(order["qty"]))

        if order["order_type"] == "Market":
            self.type = OrderType.MARKET
        elif order["order_type"] == "Limit":
            self.type = OrderType.LIMIT
        else:
            raise Exception("unknown order type")

        if "reduce_only" in order:
            self.reduce_only = order["reduce_only"]


