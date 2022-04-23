from kektrade.exceptions import UnsupportedExchange
from kektrade.exchange.backtest_inverse import BacktestInverse
from kektrade.exchange.backtest_linear import BacktestLinear
from kektrade.exchange.dryrun_inverse import DryrunInverse
from kektrade.exchange.dryrun_linear import DryrunLinear

from kektrade.misc import EnumString

class ExchangeEndpoint(EnumString):
    BinanceSpot = 'binance_spot'
    BinanceFutures = 'binance_futures'
    BinanceFuturesCoin = 'binance_futures_coin'

    BybitFutures = 'bybit_futures'
    BybitFuturesInverse = 'bybit_futures_inverse'

    BacktestLinear = 'backtest_linear'
    BacktestInverse = 'backtest_inverse'

    DryrunLinear = "dryrun_linear"
    DryrunInverse = "dryrun_inverse"



class ExchangeResolver():
    @classmethod
    def load_exchange(cls, exchange_name: ExchangeEndpoint):
        exchange = ExchangeEndpoint.from_str(exchange_name)
        if exchange == ExchangeEndpoint.BacktestInverse:
            return BacktestInverse()
        elif exchange == ExchangeEndpoint.BacktestLinear:
            return BacktestLinear()
        elif exchange == ExchangeEndpoint.DryrunLinear:
            return DryrunLinear()
        elif exchange == ExchangeEndpoint.DryrunInverse:
            return DryrunInverse()
        else:
            raise UnsupportedExchange()

