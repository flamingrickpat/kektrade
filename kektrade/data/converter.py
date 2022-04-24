from typing import List

from pandas import DataFrame, to_datetime


class Converter():
    @staticmethod
    def convert_ohlcv_list_to_dataframe(ohlcv: List[List[float]]) -> DataFrame:
        cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'funding_rate']
        df = DataFrame(ohlcv, columns=cols)

        df['date'] = to_datetime(df['date'], unit='ms', utc=True, infer_datetime_format=True)
        df = df.astype(dtype={'open': 'float', 'high': 'float', 'low': 'float', 'close': 'float',
                              'volume': 'float', 'funding_rate': 'float'})
        df["candle_count"] = 1
        return df
