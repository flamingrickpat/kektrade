# Kektrade

## Introdcution

Kektrade is a trading bot focused on trading perpetual futures on Bybit and FTX. I was not satisficed with the freedom freqtrade gives me so I decided to make this bot. The main differences are:

* **Futures only**: Spot markets are not supported.

* **Full order management**: Instead of making buy and sell signals and stoploss rules, you tell the bot which order to place at what price. This makes it possible to run complex market-making strategies that rely on limit orders.
* **Backtesting**: For good backtest results an exchange is simulated and behaves like the real one. Switching to backtest mode is like switching to a virtual exchange that calculates UPNL, liquidation price, etc. for every point in time.
* **Live optimization**: Hyperopt is a really great idea, but it's hard to backtest it. At the end you are stuck with some optimized parameters, but you don't know if you had made money if you continually applied the best hyperopt results every day. Live optimization means that when a meta strategy is running, a virtual exchange simulates all possible parameters and chooses the best - or none at all - depending on what would make the most money. 
* **OptimizeableStrategy**: In combination with live optimization you define a strategy with a search space instead of a fixed value. Much like Hyperopt.
* **Meta-Strategies** / **Subaccounts**: No strategy works at all times. Thanks to the subaccount feature of Bybit and FTX you can now run several strategies parallel. You can backtest how the performance of several meta-strategies running at the same time would look like. 
* **Telegram**: Updates and commands can be send with telegram.
* **Web-Interface**: Plots, summaries and statistics for the subaccounts can be seen in a simple web interface.

### Terminology

* **MetaStrategy**: A complete kektrade configuration. Can contain several different strategies and pairs. The goal of a meta-strategy is to find strategies with different mechanisms that cancel out drawdown periods.
* **Run**: A run is a started kektrade process. A run automatically generates a **RunID** that identifies it inside the history folder for later evaluation. A existing **RunID** can be passed to continue a run using the same SQLite database. This is useful after crashes or small fixes when continues plotting is necassary.
* **Strategy**: A strategy class with trading logic.
* **OptimizeableStrategy**: A strategy that has a search space for at least a single property. The actual parameters can be set in the config file or are dynamically determined in the optimization routine.
* **StrategyInstance**: Instance of a strategy that is bound to a pair and a subaccount.
* **Account**: Account refers to the set of all subaccounts in a config. The performance of the meta-strategy is bound to the account. It doesn't matter if the subaccounts belong to different main accounts. An account can be seen as the sum of the wallets of each subaccount.
* **Subaccount**: An exchange account that behaves like a separate account. Has it's own wallet and API key. A instance is created for every subaccount. It can be a real API, a testnet API or a simulated API. They all share the same interface. It is bound to a SQLite database that has every state change.
* **DataProvider**: The data provider is responsible for making sure all the data is available. New candles are polled and sent to the strategy instance processes with a queue.



### Configuration ###

Kektrade doesn't take any command line parameters. Since there is only a single "mode", all the configuration takes place in a JSON file. 

| Parameter | Description |
| --------- | ----------- |
|           |             |

### Exchanges ###

There are multiple exchanges available in Kektrade:

* **Bybit Perpetual**
* **Bybit Inverse**
* **FTX Perpetual**
* **Backtest Perpetual** 
* **Backtest Perpetual Realistic**







### Strategy

Static class parameters:

* **startup_candle_count **: Minimum amount of candles for indicator calculation.

  

The strategy interface defines the following functions:

* **populate_indicators**

```python
def populate_indicators(self, dataframe: DataFrame, metadata: dict, parameters: dict) -> DataFrame:
    """
    Add indicators to your dataframe or do other operations with it.
    :param dataframe: Dataframe with data from the exchange
    :param metadata: Additional information, like the currently traded pair
    :param parameter: Search space for a indicator.
    :return: a Dataframe with all mandatory indicators for the strategies
    """
    dataframe['sar'] = ta.SAR(dataframe)
 	for rsi in parameters['rsi']:
 		dataframe[f'rsi{rsi}'] = ta.RSI(dataframe, timeperiod=rsi)
    return dataframe
```

* **tick**

```python
def tick(self, dataframe: DataFrame, index: int, metadata: dict, parameter: dict, variables: dict, account: ISubaccount) -> None:
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
    c = account.get_contracts_pct_of_balance(0.01)
    if indicators.crossover(df, i, f"sma{parameter['sma']}", "close"):
        account.open_order(order_type=OrderType.MARKET, contracts=c, reduce_only=True)
```

* **populate_indicators**

```python
def populate_indicators(self, dataframe: DataFrame) -> list:
    """
    Define indicators for plotting.
    :param dataframe: Dataframe, can be used to list all columns.
    :return List of indicators and how they should appear on the plot.
    """
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
```

* **get_search_space**

```python
def get_search_space(self) -> dict:
    """
    Defines the search space for the meta-strategy
    :return Dictionary of lists with possible parameters
    """
    params = {
        "rsi": [4, 5, 6, 7, 8, 10],
        "sma": [50, 100, 150, 200, 250, 300, 350],
        "smooth": [4]
    }
    return params
```





