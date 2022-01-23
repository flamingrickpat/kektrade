import copy
import logging
from functools import cmp_to_key

import numpy as np
import pandas as pd
from pandas import DataFrame

from kektrade.exchange.interface import *
from kektrade.exchange.backtest import Backtest
from kektrade.exchange.backtest_inverse import BacktestInverse

logger = logging.getLogger(__name__)

class BacktestLinear(BacktestInverse):

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
        https://help.bybit.com/hc/en-us/articles/900000182626-Initial-Margin-USDT-Contract-
        Initial Margin is the amount of collateral required to open a position for Leverage trading. The leverage used
        is directly related to the initial margin used to maintain the position. The higher the leverage, the lower the
        initial margin required.
        Always positive.
        :param order: order
        :return: initial margin
        """
        price = order.price if order.price > 0 else self.get_close()
        return abs(order.contracts * price) / self.position.leverage

    def _get_order_cost(self, order) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/900000169703-Order-Cost-USDT-Contract-
        At the Order Confirmation Window and Order Zone, the ‘Order Cost’ is the total margin needed to open a
        particular position. It is calculated using the Initial Margin plus a 2-way taker fee. The actual fee
        charged/received depends on the execution type.
        :param order: order
        :return: order cost
        """
        price = order.price if order.price > 0 else self.get_close()
        margin = self._get_order_initial_margin(order)
        bankruptcy_price = self._get_bankruptcy_price(order.contracts)

        # Fee geht immer von Value aus!
        fee_open = abs(order.contracts * price) * self.taker_fee
        fee_close = abs(order.contracts * bankruptcy_price) * self.taker_fee

        return margin + fee_open + fee_close

    """
       ██████╗  ██████╗ ███████╗██╗████████╗██╗ ██████╗ ███╗   ██╗
       ██╔══██╗██╔═══██╗██╔════╝██║╚══██╔══╝██║██╔═══██╗████╗  ██║
       ██████╔╝██║   ██║███████╗██║   ██║   ██║██║   ██║██╔██╗ ██║
       ██╔═══╝ ██║   ██║╚════██║██║   ██║   ██║██║   ██║██║╚██╗██║
       ██║     ╚██████╔╝███████║██║   ██║   ██║╚██████╔╝██║ ╚████║
       ╚═╝      ╚═════╝ ╚══════╝╚═╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
       """

    def _get_position_initial_marign(self) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/900000182626-Initial-Margin-USDT-Contract-
        Initial Margin is the amount of collateral required to open a position for Leverage trading.
        :return: initial margin
        """
        if not self._position_open():
            return 0

        return abs(self.position.contracts * self.position.price) / self.position.leverage

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
        fee_close = abs(self.position.contracts * bankruptcy_price) * self.taker_fee
        margin = margin + fee_close
        assert margin >= 0
        return margin

    def _get_position_maintenance_margin(self) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/900000182646-Maintenance-Margin-USDT-Contract-
        Maintenance Margin is the minimum margin required to continue holding a position.
        It will increase or decrease according to the trader's selected risk limit. By default, all risk limits start
        at the lowest maintenance margin level inside each trading pair's risk limit table.
        Liquidation occurs when the isolated margin for the position is less than its maintenance margin level.
        :return: maintenance margin
        """
        if self._position_open():
            return abs(self.position.contracts) * self.position.price * self.maintenance_margin_rate
        else:
            return 0

    def _get_bankruptcy_price(self, contracts: float) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/900000181066-Bankruptcy-Price-USDT-Contract-
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
            raise Exception("not implemented")
        else:
            if self.position.leverage == 1:
                return self._get_liquidation_price()
            else:
                imr = 1 / self.leverage
                if contracts > 0:
                    bankruptcy_price = self.get_open() * (1 - imr)
                else:
                    bankruptcy_price = self.get_open() * (1 + imr)
        return bankruptcy_price

    def _get_liquidation_price(self) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/900000181046-Liquidation-Price-USDT-Contract-
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
            imr = 1 / self.leverage
            if self.position.contracts > 0:
                return self.position.price * (1 - imr + self.maintenance_margin_rate)
            else:
                return self.position.price * (1 + imr - self.maintenance_margin_rate)

    def _get_upnl(self) -> float:
        """
        https://help.bybit.com/hc/en-us/articles/900000630066-P-L-calculations-USDT-Contract-
        Unrealized P&L refers to the estimated profit and loss of an open position, also known as floating P&L.
        The unrealized P&L displayed in the position tab is calculated based on the last traded price.
        :return: upnl
        """
        if not self._position_open():
            return np.nan

        if self.position.contracts > 0:
            return abs(self.position.contracts) * (self.get_open() - self.position.price)
        elif self.position.contracts < 0:
            return abs(self.position.contracts) * (self.position.price - self.get_open())

    def _get_upnlp(self) -> float:
        """
        https://blog.bybit.com/en-us/bybit-101/how-to-understand-profit-and-loss/
        The Unrealized P&L% is essentially the Return on Investment (ROI) of the position.
        :return: upnlp
        """
        if not self._position_open():
            return np.nan

        return (self._get_upnl() / self._get_position_margin()) * 1  # 1 = 100%

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
                position_value = abs(self.position.contracts) * self.get_close()
                funding_fee = position_value * rate

                if self.position.contracts > 0:
                    if rate > 0:
                        funding_fee = funding_fee * -1
                elif self.position.contracts < 0:
                    if rate < 0:
                        funding_fee = funding_fee * -1

                execution = Execution()
                execution.pair_id = self.pair_id
                execution.subaccount_id = self.subaccount_id
                execution.optimize_id = self.optimize_id
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

    def _get_order_fee_cost(self, order: Order) -> float:
        return abs(order.contracts * order.price) * order.fee_rate

    def _get_order_rpnl_long(self, order: Order, price: float) -> float:
        return abs(order.contracts) * (self.get_open() - order.price)

    def _get_order_rpnl_short(self, order: Order, price: float) -> float:
        return abs(order.contracts) * (order.price - self.get_open())

    def _get_order_position_aep(self, order: Order) -> float:
        return ((order.contracts * order.price) + (self.position.contracts * self.position.price)) / \
                                (order.contracts + self.position.contracts)