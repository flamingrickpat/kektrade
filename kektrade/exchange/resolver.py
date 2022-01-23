from kektrade.exceptions import UnsupportedExchange
from kektrade.exchange.backtest_inverse import BacktestInverse
from kektrade.misc import EnumString

class ExchangeEndpoint(EnumString):
    BinanceSpot = 'binance_spot'
    BinanceFutures = 'binance_futures'
    BinanceFuturesCoin = 'binance_futures_coin'
    BybitFutures = 'bybit_futures'
    BybitFuturesInverse = 'bybit_futures_inverse'
    Backtest = 'backtest'
    BacktestInverse = 'backtest_inverse'

class ExchangeResolver():
    @classmethod
    def load_exchange(cls, exchange_name: ExchangeEndpoint):
        exchange = ExchangeEndpoint.from_str(exchange_name)
        if exchange == ExchangeEndpoint.BacktestInverse:
            return BacktestInverse()
        else:
            raise UnsupportedExchange()

