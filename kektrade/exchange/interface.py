from abc import ABC, abstractmethod
import logging
from pandas import DataFrame
from typing import Union

from kektrade.exceptions import UnknownExchangeParameter
from kektrade.database.types import *
from kektrade.config import RunSettings
from kektrade import utils

logger = logging.getLogger(__name__)

class IExchange(ABC):
    def __init__(self):
        self.config: Dict[str, Any] = None
        self.run_settings: RunSettings = None

        self.api_key: str = ""
        self.api_secret: str = ""

        self.subaccount_id: int = 0

        self.backtest_start: DateTime = ""
        self.backtest_end: DateTime = ""

    def set_exchange_parameters(self, **kwargs) -> None:
        """
        In the config file exchange parameters can be set.
        This function sets the parameters but only if the parameter exists in the default class.
        :param kwargs: dictionary with parameters
        """

        for key, value in kwargs.items():
            if key not in ["endpoint"]:
                if hasattr(self, key):
                    if key in ["backtest_start", "backtest_end"]:
                        setattr(self, key, utils.parse_datetime_string(value))
                    else:
                        setattr(self, key, value)
                else:
                    logger.warning(f"Exchange of type {self.__class__.__name__} has no parameter {key}")

    def set_config(self, config: Dict[str, Any], run_settings: RunSettings) -> None:
        """
        Set config and run settings.
        :param config: config
        :param run_settings: run settings
        """
        self.config = config
        self.run_settings = run_settings

    @abstractmethod
    def set_dataframe(self, dataframe: DataFrame) -> None:
        """
        Set dataframe for backtest classes.
        :param dataframe: dataframe
        """
        pass

    @abstractmethod
    def set_df_position(self, position: int) -> None:
        """
        Set position for dataframe.
        """
        pass

    @abstractmethod
    def init_exchange(self) -> None:
        """
        Perform tasks necassary to bring the exchange object into operational state.
        Called once before the first tick.
        """
        pass


    @abstractmethod
    def finalize_exchange(self) -> None:
        """
        Called once after last tick.
        """
        pass

    @abstractmethod
    def before_tick(self, i: int) -> None:
        """
        Is called before tick function of strategy.
        :param i: index in dataframe
        """
        pass

    @abstractmethod
    def after_tick(self, i: int) -> None:
        """
        Is called after tick function of strategy.
        :param i: index in dataframe
        """
        pass

    @abstractmethod
    def set_leverage(self, leverage: int) -> None:
        """
        Set leverage on exchange.
        :param leverage: leverage
        """
        pass

    @abstractmethod
    def get_open(self) -> float:
        """
        Return open price of current candle.
        :return: open price
        """
        return 0

    @abstractmethod
    def get_close(self) -> float:
        """
        Return close price of current candle.
        :return: close price
        """
        return 0

    @abstractmethod
    def get_high(self) -> float:
        """
        Return high price of current candle.
        :return: open price
        """
        return 0

    @abstractmethod
    def get_low(self) -> float:
        """
        Return low price of current candle.
        :return: close price
        """
        return 0

    @abstractmethod
    def open_order(self,
                   symbol: str,
                   order_type: OrderType,
                   contracts: float,
                   price: float = 0,
                   reduce_only: bool = False,
                   post_only: bool = False,
                   take_profit: Union[None, float] = None,
                   stop_loss: Union[None, float] = None) -> Union[None, Order]:
        """
        Create a order on the exchange.
        :param symbol: symbol
        :param order_type: order type, market or limit
        :param contracts: amount of contracts
        :param price: limit order price, can be ignored with market orders
        :param reduce_only: order can only reduce positon
        :param post_only: order will only execute if it goes into orderbook and doesn't fill immidiatly
        :param take_profit: when executed, a new take profit order will be opened at this price
        :param stop_loss: when executed, a new stop loss order will be opened at this price
        :return:
        """
        return None

    @abstractmethod
    def cancel_order(self, id: str) -> Union[None, Order]:
        """
        Cancel order with specified id
        :param id: order id
        :return: updated order object
        """
        return None

    @abstractmethod
    def cancel_all_orders(self) -> None:
        """
        Cancel all open orders
        """
        return None

    @abstractmethod
    def close_position(self) -> None:
        """
        Force close all positions.
        """
        return None

    @abstractmethod
    def set_order_price(self, id: str, price: float) -> Union[None, Order]:
        """
        Change the limit price of a order.
        :param id: order id
        :param price: new price
        :return: updated order object
        """
        return None

    @abstractmethod
    def get_position(self) -> Union[None, Position]:
        """
        Get current position or None if there is no open position.
        :return: position object
        """
        return None

    @abstractmethod
    def get_wallet(self) -> Wallet:
        """
        Get wallet object.
        :return: wallet object
        """
        pass

    @abstractmethod
    def get_contracts_percentage(self, percentage: float) -> float:
        """
        Get the amount of contracts that would use up the specified percentage of the total balance.
        Leverage is factored in
        :param percentage: percentage of total balance
        :return: contracts
        """
        return 0

    @abstractmethod
    def is_backtest(self) -> bool:
        """
        Return True if backtest.
        """
        return True