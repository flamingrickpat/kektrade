import logging
import os
from multiprocessing import Lock
from typing import Any, List, Dict
import datetime
import copy
import pytz

from kektrade import utils
from kektrade.config import RunSettings
from kektrade.data import *
from kektrade.exchange import ExchangeResolver
from kektrade.strategy import StrategyResolver
from kektrade.exchange import IExchange
from kektrade.strategy import IStrategy
from kektrade.data.dataprovider import DatetimePeriod
from kektrade.exchange import Backtest
from kektrade import utils

class SubaccountItem():
    def __init__(self, config: Dict[str, Any],
                 subaccount_config: Dict[str, Any],
                 run_settings: RunSettings,
                 file_lock: Lock,
                 start: datetime,
                 end: datetime):
        self.config: Dict[str, Any] = config
        self.subaccount_config: Dict[str, Any] = subaccount_config
        self.run_settings: RunSettings = run_settings
        self.file_lock = file_lock
        self.start: datetime = start
        self.end: datetime = end

        self.strategy: IStrategy = None
        self.exchange: IExchange = None
        self.dataprovider: DataProvider = None
        self.parameter: Dict[str, Any] = {}

        self.id: int = 0
        self.parent_subaccount_id: int = 0


    def __copy__(self):
        return SubaccountItem(
            copy.copy(self.config),
            copy.copy(self.subaccount_config),
            copy.copy(self.run_settings),
            copy.copy(self.file_lock),
            copy.copy(self.start),
            copy.copy(self.end)
        )

    def load_modules(self) -> None:
        """
        Create all the strategy and excahnge objects because they can't be pickled.
        """
        (module_path, strategy) = StrategyResolver.load_strategy(
            search_path=self.config["strategy_data_dir"],
            class_name=self.subaccount_config["strategy"]
        )
        self.strategy = strategy

        exchange = ExchangeResolver.load_exchange(exchange_name=self.subaccount_config["exchange"]["endpoint"])
        exchange_parameters = self.config.get("exchange_default_parameters", {})
        for key, value in self.subaccount_config["exchange"].items():
            exchange_parameters[key] = value
        exchange.set_exchange_parameters(**exchange_parameters)
        exchange.set_config(config=self.config, run_settings=self.run_settings)
        self.exchange = exchange

        dataprovider = DataProvider(
            search_path=self.config["data_data_dir"],
            file_lock=self.file_lock,
        )
        dataprovider.set_pairs(self.subaccount_config)
        self.dataprovider = dataprovider

    def get_required_datetimerange(self) -> DatetimePeriod:
        """
        Get the datetimes that mark the begin and end of the required data.
        In backtest mode, the parameter from the config are used.
        In live mode, the current timestamp - the required candles are used.
        :param subaccount: subaccount
        :return: DataTimerange with start and end datetime
        """
        if self.is_backtest():
            start = self.start
            end = self.end

            start -= datetime.timedelta(minutes=self.strategy.startup_candle_count *
                                                self.dataprovider.main_pair.timeframe)

            return DatetimePeriod(start, end)
        else:
            end = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
            start = end - datetime.timedelta(minutes=self.strategy.startup_candle_count *
                                                     self.dataprovider.main_pair.timeframe)
            return DatetimePeriod(start, end)

    def is_backtest(self) -> bool:
        """
        Check if the exchange is a live exchange or a simulated exchange.
        :param subaccount: subaccount
        :return: true if the exchange is a backtest exchange
        """
        return self.exchange.is_backtest()

    def is_optimization(self) -> bool:
        """
        Check if the current subaccount an optimiziation run. Optimizations don't need to be run in optimization mode.
        :return: true if subaccount is for optimization runs
        """
        return not (self.parameter is None or self.parameter == {})
