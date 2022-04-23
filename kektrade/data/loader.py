import logging
import time
from typing import Tuple
import datetime
import ccxt
import tqdm
from pandas import DataFrame

from kektrade import utils
from kektrade.data.converter import Converter
from kektrade.data.dataprovider import DatetimePeriod, PairDataInfo
from kektrade.exceptions import UnsupportedExchange
from kektrade.exchange.resolver import ExchangeEndpoint

logger = logging.getLogger(__name__)

def load_ticker(pair: PairDataInfo, data_range: DatetimePeriod) -> DataFrame:
    """
    Load the candles from exchange endpoint and convert them to OHLCV dataframe.
    Then download the funding rate history and set the funding rate at the right candle.
    Display progress with tqdm.
    :param pair: pair information with endpoint, pair and timeframe
    :param data_range: datetime range
    :return: dataframe
    """
    logger.info(f"Loading candles for {pair.pair} between {data_range.start} - {data_range.end}")
    (ccxt_exchange, package_length) = _get_ccxt_object(pair.datasource,
                                                       api_key=pair.api_key, api_secret=pair.api_secret)

    tf = utils.timeframe_int_to_str(pair.timeframe)
    tf_int = pair.timeframe
    padding = tf_int * 60

    since_ms = int((data_range.start.timestamp() - padding) * 1000)
    end_ms = int((data_range.end.timestamp() + padding) * 1000)

    loops = (((end_ms - since_ms) / 60000) / tf_int)
    with tqdm.tqdm(total=loops) as pbar:
        new = ccxt_exchange.fetch_ohlcv(pair.pair, timeframe=tf, since=since_ms, limit=package_length)
        for i in range(len(new)):
            pbar.update(1)

        while True:
            since_ms = new[-1][0] + 1
            tmp = ccxt_exchange.fetch_ohlcv(pair.pair, timeframe=tf, since=since_ms, limit=package_length)

            for i in range(len(tmp)):
                pbar.update(1)

            new += tmp

            if len(tmp) == 0 or tmp[-1][0] > end_ms:
                break



    data = new
    now = time.time() * 1000

    for i in range(len(data) - 1, -1, -1):
        data[i][0] = data[i][0] + (tf_int * 60 * 1000)
        if data[i][0] > now:
            del data[i]

    if pair.datasource in [ExchangeEndpoint.BinanceFutures, ExchangeEndpoint.BinanceFuturesCoin]:
        funding_since = data[0][0]
        funding_end = data[-1][0]
        funding = []
        while True:
            tmp = _fetch_historical_funding_rates(ccxt_exchange, pair.pair, since=funding_since, limit=1000)
            if len(tmp) > 0:
                funding += tmp
                ft = int(tmp[-1]["fundingTime"])
                funding_since = ft
                if len(tmp) < 1000 or ft > funding_end:
                    break
            else:
                break

        last_start = 0
        for i in range(len(data)):
            fr = 0.0
            for j in range(last_start, len(funding)):
                ft = int(funding[j]["fundingTime"])
                if ft == data[i][0]:
                    last_start = j
                    fr = float(funding[j]["fundingRate"])
                    break
                elif ft > data[i][0]:
                    break
            data[i].append(fr)
    else:
        logger.warning("exchange doesn't provice funding data")
        for i in range(len(data)):
            data[i].append(0.0)

    return Converter.convert_ohlcv_list_to_dataframe(data)


def _get_ccxt_object(datasource: ExchangeEndpoint, **kwargs) -> Tuple[ccxt.Exchange, int]:
    """
    Create a ccxt object based on the datasource.
    :param datasource: datasource
    :param api_key: api_key
    :param api_secret: api_secret
    :return: ccxt exchange object and length of data package from exchange
    """
    ccxt_exchange: ccxt.Exchange = None
    package_length: int = 0

    auth = {}
    if "api_key" in kwargs:
        auth["apiKey"] = kwargs["api_key"]
    if "api_secret" in kwargs:
        auth["secret"] = kwargs["api_secret"]

    if datasource in [ExchangeEndpoint.BinanceSpot, ExchangeEndpoint.BinanceFutures,
                      ExchangeEndpoint.BinanceFuturesCoin]:
        ccxt_exchange = ccxt.binance(auth)
        package_length = 500
        # ccxt_exchange.fetch_ohlcv = fetch_ohlcv_fixed

        if datasource == ExchangeEndpoint.BinanceFutures:
            ccxt_exchange.options = {
                'defaultType': 'future',
                'adjustForTimeDifference': True
            }
        elif datasource == ExchangeEndpoint.BinanceFuturesCoin:
            ccxt_exchange.options = {
                'defaultType': 'delivery',
                'adjustForTimeDifference': True
            }

    elif datasource in [ExchangeEndpoint.BybitFutures, ExchangeEndpoint.BybitFuturesInverse]:
        # raise Exception("bybit not supported since there is no funding history api endpoint")
        ccxt_exchange = ccxt.bybit(auth)
        package_length = 200
        # ccxt_exchange.parse_ohlcv = parse_ohlcv_fixed
        ccxt_exchange.options = {
            'adjustForTimeDifference': True
        }
    else:
        raise UnsupportedExchange()

    return (ccxt_exchange, package_length)



def _fetch_historical_funding_rates(self, symbol=None, since=None, limit=None, params={}):
    self.load_markets()
    market = None
    method = None
    defaultType = 'future'
    request = {}
    if symbol is not None:
        market = self.market(symbol)
        request['symbol'] = market['id']
        if market['linear']:
            defaultType = 'future'
        elif market['inverse']:
            defaultType = 'delivery'
        else:
            raise Exception(self.id + ' fetchFundingHistory() supports linear and inverse contracts only')
    if since is not None:
        request['startTime'] = since
    if limit is not None:
        request['limit'] = limit
    defaultType = self.safe_string_2(self.options, 'fetchFundingHistory', 'defaultType', defaultType)
    type = self.safe_string(params, 'type', defaultType)
    params = self.omit(params, 'type')
    if (type == 'future') or (type == 'linear'):
        method = 'fapiPublicGetFundingrate'
    elif (type == 'delivery') or (type == 'inverse'):
        method = 'dapiPublicGetFundingrate'
    else:
        raise Exception(self.id + ' fetchFundingHistory() supports linear and inverse contracts only')

    response = getattr(self, method)(self.extend(request, params))
    return response