from abc import ABC
import logging
from datetime import datetime

from kektrade.exchange.interface import IExchange

logger = logging.getLogger(__name__)


class Backtest(IExchange):
    def __init__(self):
        super(Backtest, self).__init__()

        self.initial_deposit: float = 1
        self.tradable_balance_ratio: float = 1
        self.unlimited_funds: bool = False

        self.leverage: int = 1
        self.cross_margin: bool = False
        self.maintenance_margin_rate: float = 0.005
        self.stop_market_slippage: float = 0.00025

        self.hedge_mode: int = 0
        self.hedged: bool = False

        self.maker_fee: float = 0.00025
        self.taker_fee: float = -0.00075

        self.contract_multiplier: int = 100

    def is_backtest(self):
        return True
