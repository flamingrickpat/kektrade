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
import numpy as np

logger = logging.getLogger(__name__)

# This class is a sample. Feel free to customize it.
class WavePM(IStrategy):

    def populate_variables(self, variables: Dict[str, Any]) -> None:
        variables["contracts"]  = 0

    def populate_indicators(self, dataframe: DataFrame, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> DataFrame:

        dataframe["sma"] = talib.SMA(dataframe.close, timeperiod=100)
        dataframe["rsi"] = talib.RSI(dataframe.close, timeperiod=6)

        dataframe = calculate_wavepm_bands(dataframe, wavepm_column="close", multiplikator=4, smoothing_period=0)
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

        #if variables["contracts"]  == 0:
        #    variables["contracts"] =
        m = exchange.get_contracts_percentage(50)

        if i > 10:
            if (parameter["side"] == "long"):
                if df.at[i, "close"] > df.at[i, "bb_mid_wloxp"] and exchange.get_position().contracts == 0:
                    exchange.open_order("", OrderType.MARKET, contracts=m)
                if np.isnan(df.at[i, "bb_mid_wloxp"]):
                    exchange.close_position()
                if df.at[i, "close"] < df.at[i, "bb_mid_wloxp"]:
                    exchange.close_position()

            elif (parameter["side"] == "short"):
                if df.at[i, "close"] < df.at[i, "bb_mid_wloxp"] and exchange.get_position().contracts == 0:
                    exchange.open_order("", OrderType.MARKET, contracts=-m)
                if np.isnan(df.at[i, "bb_mid_wloxp"]):
                    exchange.close_position()
                if df.at[i, "close"] > df.at[i, "bb_mid_wloxp"]:
                    exchange.close_position()

    def get_indicators(self):
        return [
            {
                "plot": True,
                "name": "rsi",
                "overlay": False,
                "scatter": False,
                "color": "red"
            },
            {
                "plot": True,
                "name": "sma",
                "overlay": True,
                "scatter": False,
                "color": "red"
            },
            {
                "plot": True,
                "name": "bb_upper_wloxp",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_mid_wloxp",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_lower_wloxp",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_upper_wloxp32",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_mid_wloxp32",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_lower_wloxp32",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },

            {
                "plot": True,
                "name": "bb_upper_wclcp",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_mid_wclcp",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_lower_wclcp",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_upper_wclcp32",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_mid_wclcp32",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_lower_wclcp32",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },

            {
                "plot": True,
                "name": "bb_upper_wlntp",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_mid_wlntp",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_lower_wlntp",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_upper_wlntp32",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_mid_wlntp32",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_lower_wlntp32",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },

            {
                "plot": True,
                "name": "rsi",
                "overlay": False,
                "scatter": False,
                "color": "red"
            },
            {
                "plot": True,
                "name": "sma",
                "overlay": True,
                "scatter": False,
                "color": "red"
            },
            {
                "plot": True,
                "name": "bb_upper_loxp",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_mid_loxp",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_lower_loxp",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_upper_loxp32",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_mid_loxp32",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },
            {
                "plot": True,
                "name": "bb_lower_loxp32",
                "overlay": True,
                "scatter": False,
                "color": "blue"
            },

            {
                "plot": True,
                "name": "bb_upper_clcp",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_mid_clcp",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_lower_clcp",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_upper_clcp32",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_mid_clcp32",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },
            {
                "plot": True,
                "name": "bb_lower_clcp32",
                "overlay": True,
                "scatter": False,
                "color": "green"
            },

            {
                "plot": True,
                "name": "bb_upper_lntp",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_mid_lntp",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_lower_lntp",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_upper_lntp32",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_mid_lntp32",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            },
            {
                "plot": True,
                "name": "bb_lower_lntp32",
                "overlay": True,
                "scatter": False,
                "color": "orange"
            }
            
            
        ]