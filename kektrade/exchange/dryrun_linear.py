from kektrade.exchange.backtest_linear import BacktestLinear

class DryrunLinear(BacktestLinear):
    pass

    def is_backtest(self):
        return False