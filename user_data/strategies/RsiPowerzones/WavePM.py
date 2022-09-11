from kektrade.exchange import *
from kektrade.database.types import *
from kektrade.strategy import *
from kektrade.misc import  *
from kektrade.indicators.wavepm import calculate_wavepm_bands

import pandas as pd
from pandas import DataFrame, Series
import logging
import talib
import tqdm
from typing import Dict, Any

logger = logging.getLogger(__name__)

# This class is a sample. Feel free to customize it.
class WavePM(IStrategy):

    def populate_variables(self, variables: Dict[str, Any]) -> None:
        variables["contracts"]  = 0

    def populate_indicators(self, dataframe: DataFrame, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> DataFrame:
        dataframe["sma_big"] = talib.SMA(dataframe.close, timeperiod=100)
        dataframe["sma_small"] = talib.SMA(dataframe.close, timeperiod=50)

        dataframe = calculate_wavepm_bands(dataframe, wavepm_column="close", multiplikator=4, smoothing_period=7)
        self.indicators = []

        for col in dataframe.columns:
            self.indicators.append(
                {
                    "plot": False,
                    "name": col,
                    "overlay": True,
                    "scatter": False,
                    "color": "red"
                }
            )

        return dataframe

    def tick(self, dataframe: DataFrame, index: int, metadata: Dict[str, Any], parameter: Dict[str, Any], variables: Dict[str, Any], exchange: IExchange) -> None:
        df = dataframe
        i = index

        if variables["contracts"]  == 0:
            variables["contracts"] = exchange.get_contracts_percentage(1)
        m = variables["contracts"]

        if i > 10:
            if (parameter["side"] == "long"):


                if df.at[i, "sma_small"] > df.at[i, "sma_big"] and df.at[i - 1, "sma_small"] < df.at[i - 1, "sma_big"]:
                    c = m
                    exchange.open_order("", OrderType.MARKET, contracts=c)
                elif df.at[i, "sma_small"] < df.at[i, "sma_big"]:
                    exchange.close_position()

            elif (parameter["side"] == "short"):


                if df.at[i, "sma_small"] > df.at[i, "sma_big"]:
                    exchange.close_position()
                elif df.at[i, "sma_small"] < df.at[i, "sma_big"] and df.at[i - 1, "sma_small"] > df.at[i - 1, "sma_big"]:
                    c = -m
                    exchange.open_order("", OrderType.MARKET, contracts=c)

    def get_indicators(self):
        return self.indicators

        return [
            {
                "plot": True,
                "name": "sma_big",
                "overlay": True,
                "scatter": False,
                "color": "red"
            },
            {
                "plot": True,
                "name": "sma_small",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            }
        ]