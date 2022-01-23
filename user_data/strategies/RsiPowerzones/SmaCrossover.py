from kektrade.exchange import *
from kektrade.database.types import *
from kektrade.strategy import *
from kektrade.misc import  *

from pandas import DataFrame
import logging
import talib
import tqdm
from typing import Dict, Any

logger = logging.getLogger(__name__)

# This class is a sample. Feel free to customize it.
class SmaCrossover(IStrategy):

    def populate_variables(self, variables: Dict[str, Any]) -> None:
        variables["rsi"] = 0

    def populate_indicators(self, dataframe: DataFrame, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> DataFrame:
        dataframe["sma_small"] = talib.SMA(dataframe.close, timeperiod=20)
        dataframe["sma_big"] = talib.SMA(dataframe.close, timeperiod=100)
        dataframe["rsi"] = talib.RSI(dataframe.close, timeperiod=100)
        return dataframe

    def tick(self, dataframe: DataFrame, index: int, metadata: Dict[str, Any], parameter: Dict[str, Any], variables: Dict[str, Any], exchange: IExchange) -> None:
        df = dataframe
        i = index

        m = 1 #exchange.get_contracts_percentage(1)

        if df.at[i, "sma_small"] > df.at[i, "sma_big"] and df.at[i - 1, "sma_small"] < df.at[i - 1, "sma_big"]:
            c = m - exchange.get_position().contracts
            exchange.open_order("", OrderType.MARKET, contracts=c)
        if df.at[i, "sma_small"] < df.at[i, "sma_big"] and df.at[i - 1, "sma_small"] > df.at[i - 1, "sma_big"]:
            c = -m - exchange.get_position().contracts
            exchange.open_order("", OrderType.MARKET, contracts=c)

    def populate_indicators(self):
        return [
            {
                "plot": True,
                "name": "sma_small",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "sma_big",
                "overlay": True,
                "scatter": False,
                "color": "red"
            },
            {
                "plot": True,
                "name": "rsi",
                "overlay": False,
                "scatter": False,
                "color": "violet"
            }
        ]