import os
from multiprocessing import Lock
from pathlib import Path
from typing import NamedTuple, Dict, List, Any
from datetime import datetime
import logging
import pandas as pd
from pandas import DataFrame

from kektrade import utils
from kektrade.exchange.resolver import ExchangeEndpoint
from kektrade.misc import EnumString

logger = logging.getLogger(__name__)

class PairDataInfo(NamedTuple):
    datasource: ExchangeEndpoint
    api_key: str
    api_secret: str
    pair: str
    timeframe: int


class DatetimePeriod(NamedTuple):
    start: datetime
    end: datetime

    def __str__(self):
        return f"{self.start.strftime('%d.%m.%Y %H:%M:%S')} - {self.end.strftime('%d.%m.%Y %H:%M:%S')}"

class DataproviderEndpoint(EnumString):
    BinanceSpot = 'binance_spot'
    BinanceFutures = 'binance_futures'
    BinanceFuturesCoin = 'binance_futures_coin'
    BybitFutures = 'bybit_futures'
    BybitFuturesInverse = 'bybit_futures_inverse'


class DataProvider():
    def __init__(self, search_path: str, file_lock: Lock):
        self.file_lock: Lock = file_lock
        self.search_path: str = search_path

        self.main_pair: PairDataInfo = None
        self.aux_pairs: List[PairDataInfo] = []
        self.pair_dataframe_dict: Dict[PairDataInfo, DataFrame] = {}


    def set_pairs(self, subaccount: Dict[str, Any]) -> None:
        """
        Parse the main pair and auxiliry pairs from the subaccount config.
        Create PairDataInfo objects and register them.
        :param subaccount:
        :return:
        """
        main_pair = PairDataInfo(
            datasource=DataproviderEndpoint.from_str(subaccount["main_pair"]["endpoint"]),
            api_key=subaccount["main_pair"].get("api_key", ""),
            api_secret=subaccount["main_pair"].get("api_secret", ""),
            pair=subaccount["main_pair"]["pair"],
            timeframe=subaccount["main_pair"]["timeframe"]
        )
        self.main_pair = main_pair

        if "aux_pairs" in subaccount:
            for aux_pair in subaccount["aux_pairs"]:
                pair = PairDataInfo(
                    datasource=DataproviderEndpoint.from_str(aux_pair["datasource"]),
                    api_key=aux_pair.get("api_key", ""),
                    api_secret=aux_pair.get("api_secret", ""),
                    pair=aux_pair["pair"],
                    timeframe=aux_pair["timeframe"]
                )
                self.aux_pairs.append(pair)


    def load_datasets_to_memory(self, range: DatetimePeriod) -> None:
        """
        Check if cached candles have all candles required for range. If not download candle data from
        datasource endpoint and save to cache.
        Then load the required part of the cached files as pandas dataframe.
        :param range: required range of data as unix timestamps
        """

        pairs: List[PairDataInfo] = [self.main_pair,] + self.aux_pairs
        for pair in pairs:
            path = DataProvider._get_data_path(self.search_path, pair)

            DataProvider._verify_cached_data(pair, range, path)
            df = DataProvider._read_ohlcv_csv(path)
            df = DataProvider._cut_range(df, range)
            self.pair_dataframe_dict[pair] = df


    def get_pair_dataframe(self, main_pair: PairDataInfo) -> DataFrame:
        """
        Return reference to dataframe in memory.
        :param main_pair: pair info
        :return: dataframe
        """
        return self.pair_dataframe_dict[main_pair]


    @staticmethod
    def _get_data_path(cache_path: str, pair: PairDataInfo) -> Path:
        """
        Construct a relative path to the cache file on the disk for a pair.
        :param pair: pair info with data source, pair and timeframe
        :return: path to csv file
        """

        return Path(os.path.join(cache_path, pair.datasource.value, utils.timeframe_int_to_str(pair.timeframe),
                                 utils.sanitize_pair(pair.pair) + '.csv'))

    @staticmethod
    def _read_ohlcv_csv(path: Path) -> DataFrame:
        """
        Read a ohclv csv file and return a pandas dataframe. Check if all required columns are defined.
        :param path: path to csv
        :return: dataframe
        """
        df = pd.read_csv(path,
                         sep=',',
                         parse_dates=['date'],
                         infer_datetime_format=True,
                         skiprows=None,
                         nrows=None,
                         index_col=0)
        cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'funding_rate']
        for col in cols:
            assert (col in df)
        return df


    @staticmethod
    def _verify_cached_data(pair: PairDataInfo, range: DatetimePeriod, path: Path) -> None:
        """
        Check the cached data and load the missing candles.
        Return without doing anything if the candles are cached.
        Load the start or end of the dataframe if candles are missing on either side.
        Load the complete dataset if the file does not exist in the cache yet.
        Save the updated dataframe and replace the original if necassary.
        :param pair: pair info
        :param range: datetime range
        :param path: path to csv cache file
        :return:
        """
        from kektrade.data.loader import load_ticker

        logger.debug(f"Data range for {pair.pair}: {range.start} - {range.end}")

        if os.path.isfile(path):
            df = DataProvider._read_ohlcv_csv(path)
            if len(df.index) > 2:

                if range.start < df.date.iloc[0]:
                    logger.info(f"Missing candles in front of cached data")
                    range_start = DatetimePeriod(
                        range.start,
                        df.date.iloc[0]
                    )
                    df_start = load_ticker(pair, range_start)
                    df = pd.concat([df_start, df])

                if range.end > df.date.iloc[-1]:
                    logger.info(f"Missing candles at back of cached data")
                    range_end = DatetimePeriod(
                        df.date.iloc[-1],
                        range.end
                    )
                    df_end = load_ticker(pair, range_end)
                    df = pd.concat([df, df_end])
            else:
                logger.info(f"No cached data")
                df = load_ticker(pair, range)
        else:
            logger.info(f"No cached data")
            df = load_ticker(pair, range)

        df = DataProvider._remove_duplicates(df)

        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path)


    @staticmethod
    def _check_range(df: DataFrame, range: DatetimePeriod) -> bool:
        """
        Check if the dataframe has datetimes within the required datetime range.
        :param df: dataframe with date column
        :param range: datetime range
        :return: true if within range, false if outside range
        """
        return (df['date'].iloc[0] <= range.start) & (df['date'].iloc[-1] >= range.end)


    @staticmethod
    def _cut_range(df: DataFrame, range: DatetimePeriod):
        """
        Return a copy of the dataframe with only the required candles cut out to save memory.
        :param df: dataframe with date column
        :param range: datetime range
        :return: dataframe where date is within the datetime range
        """
        return df[(df['date'] >= range.start) & (df['date'] <= range.end)].reset_index(drop=True)

    @staticmethod
    def _remove_duplicates(df: DataFrame) -> DataFrame:
        """
        Remove rows with duplicate dates
        :param df: dataframe
        :return: dataframe with unique rows
        """
        return df.drop_duplicates(subset=['date'], keep='last')
