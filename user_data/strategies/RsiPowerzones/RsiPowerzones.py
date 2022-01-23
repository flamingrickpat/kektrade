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
class RsiPowerzones(IStrategy):
    startup_candle_count = 200

    def populate_parameters(self) -> Dict[str, List[Any]]:
        return {
            "side": ["long", "short"],
            "sma": [100, 150],
            "rsi": [4, 6],
            #"rsi_os": [10, 15, 20, 25],
            #"rsi_ob": [90, 85, 80, 75],
        }

    def populate_variables(self, variables: Dict[str, Any]) -> None:
        variables["rsi"] = 0

    def populate_indicators(self, dataframe: DataFrame, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> DataFrame:
        for rsi in parameters["rsi"]:
            col_rsi = f"rsi{rsi}"
            dataframe[col_rsi] = talib.RSI(dataframe.close, timeperiod=rsi)
        for sma in parameters["sma"]:
            col_sma = f"sma{sma}"
            dataframe[col_sma] = talib.SMA(dataframe.close, timeperiod=sma)
        return dataframe

    def tick(self, dataframe: DataFrame, index: int, metadata: Dict[str, Any], parameter: Dict[str, Any], variables: Dict[str, Any], exchange: IExchange) -> None:
        df = dataframe
        i = index

        c = 1 #exchange.get_contracts_percentage(0.01)

        side = parameter["side"]
        rsi = parameter["rsi"]
        sma = parameter["sma"]

        col_rsi = f"rsi{rsi}"
        col_sma = f"sma{sma}"
        

        def close():
            neg_contracts = -exchange.get_position().contracts
            if neg_contracts != 0:
                exchange.open_order("", order_type=OrderType.MARKET, contracts=neg_contracts, reduce_only=True)

        if parameter is None:
            close()
        elif i > 1:
            if side == "long":
                if df.at[i, col_sma] > df.at[i - 1, col_sma]:
                    if df.at[i, col_rsi] < 25:
                        exchange.open_order("", OrderType.MARKET, contracts=c)
                    elif df.at[i, col_rsi] > 55:
                        close()
                else:
                    close()
            else:
                if df.at[i, col_sma] < df.at[i - 1, col_sma]:
                    if df.at[i, col_rsi] > 75:
                        exchange.open_order("", OrderType.MARKET, contracts=-c)
                    elif df.at[i, col_rsi] < 45:
                        close()
                else:
                    close()


    def get_indicators(self):
        res = []
        parameters = self.populate_parameters()

        for rsi in parameters["rsi"]:
            col_rsi = f"rsi{rsi}"
            res.append(
                {
                    "plot": True,
                    "name": col_rsi,
                    "overlay": False,
                    "scatter": False,
                    "color": "violet"
                }
            )
        for sma in parameters["sma"]:
            col_sma = f"sma{sma}"
            res.append(
                {
                    "plot": True,
                    "name": col_sma,
                    "overlay": True,
                    "scatter": False,
                    "color": "blue"
                }
            )

        return res