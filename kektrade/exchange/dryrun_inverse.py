from kektrade.exchange.backtest_inverse import BacktestInverse

class DryrunInverse(BacktestInverse):
    pass

    def is_backtest(self):
        return False