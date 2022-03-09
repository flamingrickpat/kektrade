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
class Sma(IStrategy):

    def populate_variables(self, variables: Dict[str, Any]) -> None:
        pass

    def populate_indicators(self, dataframe: DataFrame, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> DataFrame:
        dataframe["sma_big"] = talib.SMA(dataframe.close, timeperiod=100)
        dataframe["sma_small"] = talib.SMA(dataframe.close, timeperiod=50)
        return dataframe

    def tick(self, dataframe: DataFrame, index: int, metadata: Dict[str, Any], parameter: Dict[str, Any], variables: Dict[str, Any], exchange: IExchange) -> None:
        df = dataframe
        i = index

        m = exchange.get_contracts_percentage(1)

        if i > 10:
            if (parameter["side"] == "long"):
                if df.at[i, "sma_small"] > df.at[i, "sma_big"] and df.at[i - 1, "sma_small"] < df.at[i - 1, "sma_big"]:
                    c = m
                    exchange.open_order("", OrderType.MARKET, contracts=c)
                elif df.at[i, "sma_small"] < df.at[i, "sma_big"]:
                    c = -exchange.get_position().contracts
                    if c != 0:
                        exchange.open_order("", OrderType.MARKET, contracts=c)

            elif (parameter["side"] == "short"):
                if df.at[i, "sma_small"] > df.at[i, "sma_big"]:
                    c = -exchange.get_position().contracts
                    if c != 0:
                        exchange.open_order("", OrderType.MARKET, contracts=c)
                elif df.at[i, "sma_small"] < df.at[i, "sma_big"] and df.at[i - 1, "sma_small"] > df.at[i - 1, "sma_big"]:
                    c = -m
                    exchange.open_order("", OrderType.MARKET, contracts=c)

    def get_indicators(self):
        return [
            {
                "plot": True,
                "name": "sma",
                "overlay": True,
                "scatter": False,
                "color": "red"
            }
        ]