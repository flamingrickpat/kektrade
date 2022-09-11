import copy
import logging
from functools import cmp_to_key
from typing import List, Dict, Any, Union
import datetime
import os
import numpy as np
import pandas as pd
from pandas import DataFrame
import itertools

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session as SessionClass

from kektrade.exchange.interface import *
from kektrade.exchange.backtest import Backtest
from kektrade.exceptions import *
from kektrade import utils
from kektrade.exchange.history_meta import Versioned, versioned_session

logger = logging.getLogger(__name__)


class BacktestInverse(Backtest):
    def __init__(self):
        super().__init__()

        self.df: DataFrame = pd.DataFrame()
        self.df_position: int = 0

        self.finished: bool = False

        self.orders_open: List[Order] = []
        self.orders_canceled: List[Order] = []
        self.orders_closed: List[Order] = []
        self.orders_expired: List[Order] = []

        self.executions: List[Execution] = []
        #self.positions: List[Position] = []
        self.position: Position = Position()
        self.wallet: Wallet = Wallet()

        self.order_cnt: int = 0
        self.session: SessionClass = None


    def set_dataframe(self, dataframe: DataFrame) -> None:
        self.df = dataframe

    def set_df_position(self, position: int) -> None:
        self.df_position = position

    def init_exchange(self) -> None:
        self.wallet.subaccount_id = self.subaccount_id
        self.wallet.deposit = self.initial_deposit
        self.wallet.total_rpnl = 0
        self.wallet.account_balance = 0
        self.wallet.margin_balance = 0
        self.wallet.available_balance = 0

        self.position.subaccount_id = self.subaccount_id
        self.position.price = 0
        self.position.contracts = 0

        self.set_leverage(1)

        self.session = get_session(self.run_settings.db_path)

    def before_tick(self, i: int) -> None:
        pass

    def after_tick(self, i: int) -> None:
        if i != 0 and i != self.df_position:
            raise ExchangeException("df position mismatch")

        self.df_position += 1

        if self.df_position >= len(self.df.index) - 1:
            self.finished = True
        else:
            self._check_funding()
            self._check_liquidation()
            self._process_orders()

            self._update_position()
            self._update_wallet()

            self._copy_objects_for_history()

    def finalize_exchange(self) -> None:
        self.session.commit()

    def set_leverage(self, leverage: int) -> None:
        if self._position_open():
            logger.warning("can't change leverage while a position is open")
        else:
            self.position.leverage = leverage

    def get_open(self) -> float:
        return self.df.open.iloc[self.df_position]

    def get_close(self) -> float:
        return self.df.close.iloc[self.df_position]

    def get_high(self) -> float:
        return self.df.high.iloc[self.df_position]

    def get_low(self) -> float:
        return self.df.low.iloc[self.df_position]

    def open_order(self, symbol: str, order_type: OrderType, contracts: float, price: float = 0, reduce_only: bool = False,
                   post_only: bool = False, take_profit: Union[None, float] = None,
                   stop_loss: Union[None, float] = None) -> Union[None, Order]:
        if contracts == 0:
            raise ExchangeException("contracts must not be 0")
        elif self.hedge_mode == 1 and not ((contracts > 0 and not reduce_only) or (contracts < 0 and reduce_only)):
            raise ExchangeException("can't open short in long hedge mode")
        elif self.hedge_mode == -1 and not ((contracts < 0 and not reduce_only) or (contracts > 0 and reduce_only)):
            raise ExchangeException("can't open long in short hedge mode")
        elif order_type in [OrderType.LIMIT, OrderType.STOP_MARKET] and price == 0:
            raise ExchangeException("no price set")

        order = Order()
        order.order_id = self._get_order_id()
        order.subaccount_id = self.subaccount_id
        order.client_order_id = ""
        order.datetime = self._get_date()
        order.timestamp = self._get_timestamp()
        order.last_trade_timestamp = None
        order.status = OrderStatus.OPEN
        order.symbol = symbol
        order.order_type = order_type
        order.time_in_force = TimeInForce.GTC
        order.side = Side.BUY if contracts > 0 else Side.SELL
        order.price = price
        order.contracts = contracts
        order.cost = self._get_order_initial_margin(order)
        order.fee_currency = symbol
        order.reduce_only = reduce_only
        order.post_only = post_only
        order.hedged = self.hedged
        order.hedge_mode = self.hedge_mode
        order.insert_state = True

        if (order.order_type in [OrderType.MARKET, OrderType.STOP_MARKET]) or \
            (order.order_type == OrderType.LIMIT and ((order.contracts > 0 and order.price > self.get_close()) or
                                                      (order.contracts < 0 and order.price < self.get_close()))):
            order.taker_or_maker = TakerMakerType.TAKER
            order.fee_rate = self.taker_fee
        else:
            order.taker_or_maker = TakerMakerType.MAKER
            order.fee_rate = self.maker_fee

        self.session.add(order)

        if not self._check_order_post_only(order):
            logger.warning(f"order {order.order_id} is post-only and instantly canceled")
            self.orders_canceled.append(order)
            return None

        if not self._check_order_enough_balance(order):
            #logger.warning(f"not enough balance for order {order.order_id}")
            self.orders_canceled.append(order)
            return None

        self.orders_open.append(order)
        return order

    def cancel_order(self, id: str) -> Union[None, Order]:
        for order in self.orders_open:
            if order.order_id == id:
                order.status = OrderStatus.CANCELED
                return order

    def cancel_all_orders(self) -> None:
        for order in self.orders_open:
            order.status = OrderStatus.CANCELED

    def set_order_price(self, id: str, price: float) -> Union[None, Order]:
        for order in self.orders_open:
            if order.order_id == id:
                order.price = price
                return order

    def get_position(self) -> Union[None, Position]:
        return self.position

    def get_wallet(self) -> Wallet:
        return self.wallet

    def get_contracts_percentage(self, percentage: float) -> float:
        price = self.get_close()
        tmp = Order()
        tmp.price = price
        tmp.contracts = 1
        price_one_contract = self._get_order_initial_margin(tmp)
        available = self._get_account_balance() * (percentage / 100)
        contracts = available / price_one_contract
        return contracts

    def close_position(self):
        if self.get_position().contracts != 0:
            self.open_order("", order_type=OrderType.MARKET, contracts=-self.get_position().contracts)

    """
    ██╗███╗   ██╗████████╗███████╗██████╗ ███╗   ██╗ █████╗ ██╗     
    ██║████╗  ██║╚══██╔══╝██╔════╝██╔══██╗████╗  ██║██╔══██╗██║     
    ██║██╔██╗ ██║   ██║   █████╗  ██████╔╝██╔██╗ ██║███████║██║     
    ██║██║╚██╗██║   ██║   ██╔══╝  ██╔══██╗██║╚██╗██║██╔══██║██║     
    ██║██║ ╚████║   ██║   ███████╗██║  ██║██║ ╚████║██║  ██║███████╗
    ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝
    """

    def _get_order_id(self) -> int:
        """
        Generate a new order id. Starts with 1 and increments by 1 for every new order.
        :return: order id
        """
        self.order_cnt += 1
        return self.order_cnt

    def _position_open(self) -> bool:
        """
        Check if there currently is a position open
        :return: bool
        """
        return self.position.contracts != 0

    def _get_date(self) -> datetime.datetime:
        """
        Get the current datetime in dataframe as python datetime
        :return: python dt
        """
        return utils.pdts_to_pydt(self.df.at[self.df_position, "date"])

    def _get_timestamp(self) -> int:
        """
        Get the current datetime in dataframe as unix timestamp.
        :return: unix timestamp in seconds
        """
        return int(self._get_date().timestamp())


    """
    ██╗    ██╗ █████╗ ██╗     ██╗     ███████╗████████╗
    ██║    ██║██╔══██╗██║     ██║     ██╔════╝╚══██╔══╝
    ██║ █╗ ██║███████║██║     ██║     █████╗     ██║   
    ██║███╗██║██╔══██║██║     ██║     ██╔══╝     ██║   
    ╚███╔███╔╝██║  ██║███████╗███████╗███████╗   ██║   
     ╚══╝╚══╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝   ╚═╝   
    """

    def _get_account_balance(self) -> float:
        """
        Account Balnace = deposit + total rpnl
        :return: account balance
        """
        if self.unlimited_funds:
            return self.wallet.deposit

        return self.wallet.deposit + self.wallet.total_rpnl

    def _get_margin_balance(self) -> float:
        """
        Margin Balance = account balance + unrealized profit
        :return: margin balance
        """
        if self.unlimited_funds:
            return self.wallet.deposit

        return self._get_account_balance() + np.nan_to_num(self._get_upnl(), nan=0)

    def _get_available_balance(self) -> float:
        """
        Availible Balance = account balance - upnl - position margin - order margin
        :return: available balance
        """
        if self.unlimited_funds:
            return self.wallet.deposit

        position_margin = self._get_position_margin()
        order_margin = self._get_order_margin()
        return self._get_margin_balance() - position_margin - order_margin


    def _update_wallet(self) -> None:
        """
        Update the wallet object with the current values.
        """
        self.wallet.account_balance = self._get_account_balance()
        self.wallet.available_balance = self._get_available_balance()
        self.wallet.margin_balance = self._get_margin_balance()
        self.wallet.order_margin = self._get_order_margin()
        self.wallet.position_margin = self._get_position_margin()

    """
     ██████╗ ██████╗ ██████╗ ███████╗██████╗ 
    ██╔═══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗
    ██║   ██║██████╔╝██║  ██║█████╗  ██████╔╝
    ██║   ██║██╔══██╗██║  ██║██╔══╝  ██╔══██╗
    ╚██████╔╝██║  ██║██████╔╝███████╗██║  ██║
     ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═╝
    """

    def _get_order_initial_margin(self, order: Order) -> float:
        """
        https://blog.bybit.com/en-us/bybit-101/how-to-calculate-order-cost/
        Initial margin is calculated by the order quantity and estimated execution price. For market orders, the
        estimated execution price is the best bid or ask price. For limit orders, the estimated entry price is
        usually the order price.
        Always positive.
        :param order: order
        :return: initial margin
        """
        price = order.price if order.price > 0 else self.get_close()
        return abs(order.contracts) / (self.leverage * price)

    def _get_order_cost(self, order) -> float:
        """
        https://blog.bybit.com/en-us/bybit-101/how-to-calculate-order-cost/
        Order Cost is the total margin required to open a new position. It consists of the estimated initial margin and
        an estimated 2-way taker fees for opening and closing the position. Order cost is denominated in BTC terms for
        BTCUSD contract, and in USDT terms for BTCUSDT contract.
        :param order: order
        :return: order cost
        """
        margin = self._get_order_initial_margin(order)
        bankruptcy_price = self._get_bankruptcy_price(order.contracts)

        # Fee geht immer von Value aus!
        fee_open = margin * self.taker_fee
        fee_close = abs(order.contracts / bankruptcy_price) * self.taker_fee

        return margin + fee_open + fee_close

    def _get_order_margin(self) -> float:
        """
        Sums the initial margin of all opened orders.
        :return: total order margin
        """
        margin = 0
        for order in self.orders_open:
            margin += self._get_order_initial_margin(order)
        return margin

    def _is_order_reducing(self, order: Order) -> bool:
        """
        Check if the order would reduce the current position (-100 -> -10 is reducing)
        :param order: order
        :return: bool
        """
        reduce = False
        if order.reduce_only:
            reduce = True
        if abs(self.position.contracts + order.contracts) < abs(self.position.contracts):
            reduce = True
        return reduce

    def _check_order_enough_balance(self, order: Order) -> bool:
        """
        Check if the order can be executed with the current availible balance.
        :param order:
        :return:
        """
        if not self._is_order_reducing(order):
            order_cost = self._get_order_initial_margin(order)
            if order_cost > self._get_available_balance() and not self.unlimited_funds:
                return False
        return True

    def _check_order_reduce_only(self, order: Order) -> bool:
        """
        Check if the order can be executed considering the reduce only parameter.
        :param order: order
        :return: bool
        """
        if order.reduce_only:
            if (order.contracts > 0 and self.position.contracts > 0) or \
                    (order.contracts > 0 and self.position.contracts > 0) or \
                    (self.position.contracts == 0):
                return False
        return True

    def _check_order_lifetime(self, order: Order) -> bool:
        """
        Check if the order should be canceled.
        :param order: order
        :return: bool
        """
        return True

    def _check_order_post_only(self, order: Order) -> bool:
        """
        Check if the order can be opened considerung the post only parameter.
        :param order: order
        :return: bool
        """
        current_price = self.get_open()
        if order.post_only:
            if (order.order_type == OrderType.LIMIT and ((order.contracts > 0 and order.price > current_price) or
                                                         (order.contracts < 0 and order.price < current_price))) or \
                (order.order_type == OrderType.STOP_MARKET and ((order.contracts > 0 and order.price < current_price) or
                                                                (order.contracts < 0 and order.price > current_price))):
                return False
        return True

    """
    ██████╗  ██████╗ ███████╗██╗████████╗██╗ ██████╗ ███╗   ██╗
    ██╔══██╗██╔═══██╗██╔════╝██║╚══██╔══╝██║██╔═══██╗████╗  ██║
    ██████╔╝██║   ██║███████╗██║   ██║   ██║██║   ██║██╔██╗ ██║
    ██╔═══╝ ██║   ██║╚════██║██║   ██║   ██║██║   ██║██║╚██╗██║
    ██║     ╚██████╔╝███████║██║   ██║   ██║╚██████╔╝██║ ╚████║
    ╚═╝      ╚═════╝ ╚══════╝╚═╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
    """
    def _reset_position(self) -> None:
        """
        Clean up position obejct after closing position.
        :return:
        """
        self.position.price = 0
        self.position.contracts = 0

    def _get_position_initial_marign(self) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/360039261174-Initial-Margin-Inverse-Contract-
        Initial Margin is the amount of collateral required to open a position for Leverage trading.
        :return: initial margin
        """
        if not self._position_open():
            return 0

        return abs(self.position.contracts) / (self.position.price * self.position.leverage)

    def _get_position_margin(self) -> float:
        """
        Initial margin of the postion + worst-case exit fee
        Always positive
        :return: position margin
        """
        if not self._position_open():
            return 0

        margin = self._get_position_initial_marign()
        bankruptcy_price = self._get_bankruptcy_price(self.position.contracts)
        fee_close = abs(self.position.contracts / bankruptcy_price) * self.taker_fee
        margin = margin + fee_close
        assert margin >= 0
        return margin

    def _get_position_maintenance_margin(self) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/360039261214-Maintenance-Margin-Inverse-Contract-
        Maintenance Margin is the minimum margin required to continue holding a position.
        For perpetual contracts, the maintenance margin base rate is 0.5% for BTC and 1% for ETH, EOS and XRP of the
        contract value when opening a position. It will increase or decrease accordingly as risk limit changes.
        Liquidation occurs when the isolated margin for the position is less than its maintenance margin level.
        :return: maintenance margin
        """
        if self._position_open():
            return (abs(self.position.contracts) / self.position.price) * self.maintenance_margin_rate
        else:
            return 0

    def _get_bankruptcy_price(self, contracts: float) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/360039749813-What-is-Bankruptcy-Price-Inverse-Contract-
        Bankruptcy Price is a price level that indicates you have lost all your initial margin.
        Upon liquidation, the liquidated position will be closed at the Bankruptcy Price, and this means that you have
        lost all the position margin. If the liquidated position has its final liquidation price better than the
        bankruptcy price, the excess margin will be contributed to the Insurance Fund. Vice versa, if the liquidated
        position has its final liquidation price worse than the bankruptcy price, the Insurance fund will cover the
        loss gap.
        Bankruptcy Price is also used to calculate the fee to close position, reflected in the Order Cost.
        :param contracts: amount of contracts
        :return: bankruptcy price
        """
        if not self._position_open():
            return 0

        if self.cross_margin:
            value = abs(contracts / self.position.price)
            margin = value / self.position.leverage
            fee = margin * self.taker_fee

            if contracts > 0:
                bankruptcy_price = (1.00075 * contracts) / (value + (self._get_account_balance() - margin - fee))
            else:
                bankruptcy_price = (0.99925 * contracts) / (value - (self._get_account_balance() - margin - fee))
        else:
            if self.position.leverage == 1:
                return self._get_liquidation_price()
            else:
                if contracts > 0:
                    bankruptcy_price = self.get_open() * (self.position.leverage / (self.position.leverage + 1))
                else:
                    bankruptcy_price = self.get_open() * (self.position.leverage / (self.position.leverage - 1))
        return bankruptcy_price


    def _get_liquidation_price(self) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/360039261334-How-to-calculate-Liquidation-Price-Inverse-Contract-
        Liquidation is triggered when the Mark Price hits the Liquidation Price. The minimum maintenance margin rate
        for perpetual contracts on Bybit is 0.5%  for BTC and 1% for ETH, EOS and XRP. Margin requirements will
        increase or decrease accordingly as risk limit changes.
        Liquidation price is calculated based on the trader's selected leverage, maintenance margin and entry price.
        :return:
        """
        if not self._position_open():
            return 0

        if self.cross_margin:
            raise Exception("not implemented")
        else:
            if self.position.contracts > 0:
                return (self.position.price * self.position.leverage) / (self.position.leverage + 1 - (self.maintenance_margin_rate * self.position.leverage))
            else:
                return (self.position.price * self.position.leverage) / (self.position.leverage - 1 + (self.maintenance_margin_rate * self.position.leverage))


    def _get_upnl(self) -> float:
        """
        https://blog.bybit.com/en-us/bybit-101/how-to-understand-profit-and-loss/
        Unrealized P&L refers to the estimated profit and loss of an open position, also known as floating P&L.
        The unrealized P&L displayed in the position tab is calculated based on the last traded price.
        :return: upnl
        """
        if not self._position_open():
            return np.nan

        if self.position.contracts > 0:
            return self.position.contracts * ((1 / self.position.price) - (1 / self.get_open()))
        elif self.position.contracts < 0:
            return self.position.contracts * ((1 / self.position.price) - (1 / self.get_open()))

    def _get_upnlp(self) -> float:
        """
        https://blog.bybit.com/en-us/bybit-101/how-to-understand-profit-and-loss/
        The Unrealized P&L% is essentially the Return on Investment (ROI) of the position.
        :return: upnlp
        """
        if not self._position_open():
            return np.nan

        return (self._get_upnl() / self._get_position_margin()) * 1 # 1 = 100%

    def _update_position(self) -> None:
        """
        Update position object with current values.
        """
        self.position.collateral = self._get_position_margin()
        self.position.initialMargin = self._get_position_initial_marign()
        self.position.maintenanceMargin = self._get_position_maintenance_margin()
        self.position.unrealizedPnl = self._get_upnl()
        self.position.unrealizedPnlPercentage = self._get_upnlp()
        self.position.liquidationPrice = self._get_liquidation_price()
        self.position.bankruptcyPrice = self._get_bankruptcy_price(self.position.contracts)


    """
    ██╗      ██████╗  ██████╗ ██╗ ██████╗
    ██║     ██╔═══██╗██╔════╝ ██║██╔════╝
    ██║     ██║   ██║██║  ███╗██║██║     
    ██║     ██║   ██║██║   ██║██║██║     
    ███████╗╚██████╔╝╚██████╔╝██║╚██████╗
    ╚══════╝ ╚═════╝  ╚═════╝ ╚═╝ ╚═════╝
    """

    def _check_funding(self) -> None:
        """
        https://help.bybit.com/hc/en-us/articles/360039261134-Funding-fee-calculation
        Check if the current candle has a funding rate set.
        If a position is open, calculate the funding cost and create a new execution event.
        """
        if self._position_open():
            rate = self.df.at[self.df_position, "funding_rate"]
            if (not np.isnan(rate) and rate != 0):
                position_value = abs(self.position.contracts) / self.get_close()
                funding_fee = abs(position_value * rate)

                if self.position.contracts > 0:
                    if rate > 0:
                        funding_fee = funding_fee * -1
                elif self.position.contracts < 0:
                    if rate < 0:
                        funding_fee = funding_fee * -1

                execution = Execution()
                execution.subaccount_id = self.subaccount_id
                execution.execution_type = ExecutionType.FUNDING
                execution.execution_id = self._get_order_id()
                execution.order_id = 0
                execution.datetime = self._get_date()
                execution.timestamp = self._get_timestamp()
                # execution.symbol = self.position.symbol
                execution.price = self.get_close()
                execution.contracts = 0
                execution.cost = funding_fee
                execution.reduce_or_expand = ReduceExpandType.REDUCE
                execution.fee_rate = 0
                execution.fee_cost = 0
                self.session.add(execution)

                self.wallet.total_rpnl += execution.cost



    def _check_liquidation(self) -> None:
        """
        # https://help.bybit.com/hc/en-us/articles/360039261334-How-to-calculate-Liquidation-Price-Inverse-Contract-
        # Check if the position needs to be liquidated.
        """
        if self.position.contracts == 0:
            return
        else:
            if self.cross_margin:
                raise ExchangeException("not implemented")
            else:
                liquidated = False
                if self.position.contracts > 0:
                    if self.df.at[self.df_position, "low"] < self._get_liquidation_price():
                        liquidated = True
                elif self.position.contracts < 0:
                    if self.df.at[self.df_position, "high"] > self._get_liquidation_price():
                        liquidated = True

                if liquidated:
                    execution = Execution()
                    execution.subaccount_id = self.subaccount_id
                    execution.execution_type = ExecutionType.LIQUIDATION
                    execution.execution_id = self._get_order_id()
                    execution.order_id = 0
                    execution.datetime = self._get_date()
                    execution.timestamp = self._get_timestamp()
                    #execution.symbol = self.position.symbol
                    execution.price = self._get_liquidation_price()
                    execution.contracts = self.position.contracts
                    execution.cost = -self._get_position_margin()
                    execution.reduce_or_expand = ReduceExpandType.REDUCE
                    execution.fee_rate = 0
                    execution.fee_cost = 0
                    self.session.add(execution)

                    self.wallet.total_rpnl += execution.cost
                    self._reset_position()

    def _process_orders(self) -> None:
        """
        Go over open order and check if they need to be executed or canceled.

        :return:
        """
        open = self.df.open.iloc[self.df_position]
        high = self.df.high.iloc[self.df_position]
        low = self.df.low.iloc[self.df_position]

        # Order mit Abstand zu Open sortieren, falls mehre in der selben Candle ausgeführt werden
        def compare(item1, item2):
            # Markets immer ganz unten
            if item1.price is None:
                return -1
            if item2.price is None:
                return 1

            dist1 = abs(item1.price - open)
            dist2 = abs(item2.price - open)

            if dist1 < dist2:
                return -1
            elif dist1 > dist2:
                return 1
            else:
                return 0

        tmp = self.orders_open.copy()
        tmp.sort(key=cmp_to_key(compare))
        for order in tmp:
            if order.status == OrderStatus.CANCELED:
                self._cancel_order(order)
            elif order.status == OrderStatus.OPEN:
                cancel: bool = False

                self.orders_open.remove(order)
                if not self._check_order_enough_balance(order):
                    #logger.info(f"not enough balance for order {order.order_id}")
                    cancel = True
                self.orders_open.append(order)

                if not self._check_order_reduce_only(order):
                    logger.info(f"reduce only order not possible for order {order.order_id}")
                    cancel = True

                if not self._check_order_lifetime(order):
                    logger.info(f"order {order.order_id} is expired and will be canceled")
                    cancel = True

                if cancel:
                    self._cancel_order(order)
                else:
                    limit_filled: bool = False
                    if order.order_type == OrderType.LIMIT or order.order_type == OrderType.STOP_MARKET:
                        if order.contracts > 0:
                            if low < order.price < high:
                                limit_filled = True
                        if order.contracts < 0:
                            if low < order.price < high:
                                limit_filled = True

                    if (limit_filled or order.order_type == OrderType.MARKET):
                        order.status = OrderStatus.CLOSED
                        order.last_trade_datetime = self._get_date()
                        order.last_trade_timestamp = self._get_timestamp()

                        if (order.order_type == OrderType.MARKET):
                            order.price = self.get_open()
                        elif (order.order_type == OrderType.STOP_MARKET):
                            slippage = abs(order.price * self.stop_market_slippage)
                            if order.contracts > 0:
                                order.price += slippage
                            else:
                                order.price -= slippage

                        self._execute_order(order)

    def _cancel_order(self, order: Order) -> None:
        """
        Set status to cancel and move to seperate list.
        :param order: order
        """
        order.status = OrderStatus.CANCELED
        self.orders_canceled.append(order)
        self.orders_open.remove(order)


    def _get_order_fee_cost(self, order: Order) -> float:
        return abs(order.contracts / order.price) * order.fee_rate

    def _get_order_rpnl_long(self, order: Order, price: float) -> float:
        return self.position.contracts * ((1 / self.position.price) - (1 / self.get_open()))

    def _get_order_rpnl_short(self, order: Order, price: float) -> float:
        return self.position.contracts * ((1 / self.position.price) - (1 / self.get_open()))

    def _get_order_position_aep(self, order: Order) -> float:
        return (self.position.contracts + order.contracts) / ((self.position.contracts / self.position.price) +
                                                              (order.contracts / order.price))

    def _execute_order(self, order: Order) -> None:
        self.orders_closed.append(order)
        self.orders_open.remove(order)

        # Reduce only können keinen Seitenwechsel machen!
        if order.reduce_only:
            if abs(order.contracts) > abs(self.position.contracts):
                if (order.contracts > 0 and self.position.contracts < 0) or (order.contracts < 0 and self.position.contracts > 0):
                    order.contracts = -self.position.contracts

        def expand(order_tmp: Order):
            execution = Execution()
            execution.subaccount_id = self.subaccount_id
            execution.execution_type = ExecutionType.TRADE
            execution.execution_id = self._get_order_id()
            execution.order_id = order_tmp.order_id
            execution.datetime = self._get_date()
            execution.timestamp = self._get_timestamp()
            #execution.symbol = self.position.symbol
            execution.price = order_tmp.price
            execution.contracts = order_tmp.contracts
            execution.cost = self._get_order_initial_margin(order_tmp)
            execution.reduce_or_expand = ReduceExpandType.EXPAND
            execution.fee_rate = order_tmp.fee_rate
            execution.fee_cost = self._get_order_fee_cost(order_tmp)
            execution.taker_or_maker = order.taker_or_maker
            self.session.add(execution)

            if self.position.contracts == 0:
                self.position.price = order_tmp.price
                self.position.contracts = order_tmp.contracts
            else:
                self.position.price = self._get_order_position_aep(order)
                self.position.contracts += order_tmp.contracts

            self.wallet.total_rpnl += execution.fee_cost

        def reduce(order_tmp: Order):
            execution = Execution()
            execution.subaccount_id = self.subaccount_id
            execution.execution_type = ExecutionType.TRADE
            execution.execution_id = self._get_order_id()
            execution.order_id = order_tmp.order_id
            execution.datetime = self._get_date()
            execution.timestamp = self._get_timestamp()
            #execution.symbol = self.position.symbol
            execution.price = order_tmp.price
            execution.contracts = order_tmp.contracts
            execution.cost = 0
            execution.reduce_or_expand = ReduceExpandType.REDUCE
            execution.fee_rate = order_tmp.fee_rate
            execution.fee_cost = self._get_order_fee_cost(order_tmp)
            execution.taker_or_maker = order.taker_or_maker

            price = order_tmp.price if order.order_type == OrderType.LIMIT else self.get_open()
            if self.position.contracts > 0:
                rpnl = self._get_order_rpnl_long(order_tmp, price)
            else:
                rpnl = self._get_order_rpnl_short(order_tmp, price)

            execution.cost = rpnl
            self.session.add(execution)

            self.wallet.total_rpnl += execution.fee_cost
            self.position.contracts += order_tmp.contracts
            self.wallet.total_rpnl += rpnl

            if self.position.contracts == 0:
                self._reset_position()

        # Seitenwechel
        if (self.position.contracts > 0 and (self.position.contracts + order.contracts) < 0) or \
            (self.position.contracts < 0 and (self.position.contracts + order.contracts) > 0):
            # Kopie vom Order machen, damit auf 0 reducen
            # Dann im Original die verbleibenden Contracts anpassen
            reduction = copy.copy(order)
            reduction.order_id = self._get_order_id()
            reduction.contracts = -self.position.contracts
            self.session.add(reduction)
            order.contracts += self.position.contracts
            reduce(reduction)

        if abs(self.position.contracts + order.contracts) > abs(self.position.contracts):
            expand(order)
        elif abs(self.position.contracts + order.contracts) < abs(self.position.contracts):
            reduce(order)

    def _copy_objects_for_history(self):
        self.wallet.datetime = self._get_date()
        self.wallet.timestamp = self._get_timestamp()
        tmp = copy_sqla_object(self.wallet)
        self.session.add(tmp)

        self.position.datetime = self._get_date()
        self.position.timestamp = self._get_timestamp()
        tmp = copy_sqla_object(self.position)
        self.session.add(tmp)

        for order in itertools.chain(self.orders_open): #, self.orders_canceled, self.orders_closed, self.orders_expired):
            tmp = copy_sqla_object(order)
            tmp.insert_state = False
            tmp.datetime = self._get_date()
            tmp.timestamp = self._get_timestamp()
            self.session.add(tmp)

        self.orders_closed = []
        self.orders_canceled = []
        self.orders_expired = []







