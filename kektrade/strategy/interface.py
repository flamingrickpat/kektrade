import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List

from pandas import DataFrame

from kektrade.exchange import IExchange

logger = logging.getLogger(__name__)


class IStrategy(ABC):
    # Count of candles the strategy requires before producing valid signals
    startup_candle_count: int = 0

    def populate_parameters(self) -> Dict[str, List[Any]]:
        """
        Define the optimizable parameters this strategy has.
        """
        return {}

    @abstractmethod
    def populate_variables(self, variables: Dict[str, Any]) -> None:
        """
        Initialize variables that are persistent between tick calls.
        :param variables: Dictionary with values
        """
        pass

    @abstractmethod
    def populate_indicators(self, dataframe: DataFrame,
                            metadata: Dict[str, Any],
                            parameters: Dict[str, Any]) -> DataFrame:
        """
        Add indicators to your dataframe or do other operations with it.
        :param dataframe: Dataframe with data from the exchange
        :param metadata: Additional information, like the currently traded pair
        :param parameter: Search space for a indicator.
        :return: a Dataframe with all mandatory indicators for the strategies
        """
        pass

    @abstractmethod
    def tick(self, dataframe: DataFrame,
             index: int,
             metadata: Dict[str, Any],
             parameter: Dict[str, Any],
             variables: Dict[str, Any],
             exchange: IExchange) -> None:
        """
    	Perform the order management and trading logic.
    	This function is called as soon as a new candle is appended to the dataframe or in a loop in case of backtest.
    	:param dataframe: Dataframe with indicators
    	:param index: Current position in dataframe. Exists for backtest compatibility. In Live it is len(dataframe) -1.
    	:param metadata: Additional information, like the currently traded pair
    	:param parameter: Single parameter configuration from all possible combinations
    	:param variables: Dictionary to store information about the current state. Stays persistent between tick calls.
    	:param exchange: Exchange object. Provides direct interface to exchange.
        """
        pass


    def get_indicators(self) -> List[Dict[str, Any]]:
        """
        Return a list of indicators for plotting.
        :return: list of dictionaries with plotting details
        """
        return []
