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
        dataframe["sma_big"] = talib.SMA(dataframe.close, timeperiod=10)
        return dataframe

    def tick(self, dataframe: DataFrame, index: int, metadata: Dict[str, Any], parameter: Dict[str, Any], variables: Dict[str, Any], exchange: IExchange) -> None:
        df = dataframe
        i = index

        m = 1

        if i > 10:
            if df.at[i, "sma_big"] > df.at[i - 1, "sma_big"]:
                c = m
                exchange.open_order("", OrderType.MARKET, contracts=c)
            elif df.at[i, "sma_big"] < df.at[i - 1, "sma_big"]:
                c = -m
                exchange.open_order("", OrderType.MARKET, contracts=c)

    def get_indicators(self):
        return [
            {
                "plot": True,
                "name": "sma_big",
                "overlay": True,
                "scatter": False,
                "color": "red"
            }
        ]