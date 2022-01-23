import logging
from typing import Dict, Any, List

from pandas import DataFrame

from kektrade.exchange import IExchange
from kektrade.strategy import IStrategy

logger = logging.getLogger(__name__)

# This class is a sample. Feel free to customize it.
class DefaultStrategy(IStrategy):

    def populate_parameters(self) -> Dict[str, List[Any]]:
        return {}

    def populate_indicators(self, dataframe: DataFrame, metadata: Dict[str, Any], parameters: Dict[str, Any]) -> DataFrame:
        return dataframe

    def tick(self, dataframe: DataFrame, index: int, metadata: Dict[str, Any], parameter: Dict[str, Any], variables: Dict[str, Any], exchange: IExchange) -> None:
        return None

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
                "color": "blue"
            }
        ]