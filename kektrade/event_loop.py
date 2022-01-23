import datetime
import time
from typing import Dict, List, Any
import logging
import os
from pathlib import Path
import copy

from pandas import DataFrame
import tqdm

from kektrade.data.dataprovider import DatetimePeriod
from kektrade.exchange import Backtest
from kektrade.database.types import Subaccount, Pair, get_engine
from kektrade.database.types import get_session
from kektrade.plotting import PlotterSubaccount
from kektrade.optimization import Optimizer
from kektrade.subaccount import SubaccountItem
from kektrade import utils

logger = logging.getLogger(__name__)

def start_eventloop(subaccount: SubaccountItem) -> int:
    """
    Start the main loop by passing a subaccount object.
    Can be called with multiprocessing.
    :param subaccount: Subaccount
    :return: database id of subaccount: int
    """
    subaccount.load_modules()
    loop = EventLoop(subaccount)
    return loop.start()


class EventLoop():
    """
    The EventLoop class handles the logic for a single strategy bound to a single subaccount.
    """

    def __init__(self, subaccount: SubaccountItem):
        self.subaccount: SubaccountItem = subaccount
        self.current_candle_timestamp: int = 0
        self.df_position: int = 0
        self.progress_bar: tqdm.tqdm = None

        self.subaccount_id: int = 0
        self.optimize_id: int = 0
        self.pair_id: int = 0

        self.optimized_parameter: Dict[str, Any] = None
        self.recalculate_inidcators: bool = True

    def start(self) -> int:
        """
        This is the main loop. At first the necassary candles are loaded.
        In a endless loop check if there is a new candle. If there is one, recalculate the indicators and call the
        tick function of the strategy.
        If the strategy is a meta strategy, run the optimization process async in the background and retrieve the
        optimized parameters once they are ready.
        :return: database id of subaccount: int
        """

        subaccount = self.subaccount
        self._init_database()
        self._load_candles()
        self._init_exchange()

        metadata = {}
        variables = {}
        subaccount.strategy.populate_variables(variables)

        if not self.subaccount.is_optimization():
            logger.info(f"=== Start event loop for subaccount {self.subaccount.subaccount_config['subaccount_id']} ===")

        time.sleep(1)

        if self.subaccount.is_backtest() and not self.subaccount.is_optimization():
            pbar = tqdm.tqdm(total=self._get_df_length())

        self._init_optimize()
        self._init_progress()
        while self._active():
            index = self._get_index()

            if self.subaccount.is_backtest() and not self.subaccount.is_optimization():
                pbar.update(1)

            self._load_new_candles()

            self._optimize_start()
            parameter = self._optimize_get_parameter()
            parameters_indicators = self._optimize_get_parameters_indicators()
            subaccount.exchange.before_tick(index)

            if parameter:
                df = self._get_main_df()
                df = self._populate_indicators(subaccount, df, metadata, parameters_indicators)

                subaccount.strategy.tick(dataframe=df, index=index, metadata=metadata, parameter=parameter,
                                         variables=variables, exchange=subaccount.exchange)
            else:
                subaccount.exchange.close_position()

            subaccount.exchange.after_tick(index)
            self.df_position += 1
            self._progress_step()

        if self.subaccount.is_backtest() and not self.subaccount.is_optimization():
            pbar.close()

        self.subaccount.exchange.finalize_exchange()
        self._plot_subaccount()
        self._free_progress()

        if not self.subaccount.is_optimization():
            logger.info(f"=== Finished event loop for subaccount {self.subaccount.subaccount_config['subaccount_id']} ===")

        return self.subaccount.id

    def _init_database(self) -> None:
        """
        Open the database, create the "subaccount", "pair", "subaccount_pair" entries and save the row IDs in the local
        objects.
        """
        session = get_session(self.subaccount.run_settings.db_path)

        subaccount = Subaccount()
        subaccount.subaccount_id = self.subaccount.subaccount_config["subaccount_id"]
        subaccount.strategy = self.subaccount.subaccount_config["strategy"]
        subaccount.parameter = str(self.subaccount.parameter)
        subaccount.parent_subaccount_id = self.subaccount.parent_subaccount_id
        subaccount.start = self.subaccount.start
        subaccount.end = self.subaccount.end
        session.add(subaccount)
        session.commit()

        self.subaccount.id = subaccount.id

        pair = Pair()
        pair.pair = self.subaccount.dataprovider.main_pair.pair
        pair.timeframe = self.subaccount.dataprovider.main_pair.timeframe
        pair.datasource = self.subaccount.dataprovider.main_pair.datasource.value
        pair.subaccount_id = subaccount.id
        pair.optimize_configuration_id = self.optimize_id
        pair.fl_main = not self.subaccount.is_optimization()
        session.add(pair)
        session.commit()

        self.subaccount_id = subaccount.id
        self.pair_id = pair.id

    def _load_candles(self) -> None:
        """
        Make sure that the required data is fully cached on the disk and loaded as pandas dataframe
        in the dataprovider object.
        """
        with self.subaccount.file_lock:
            range = self.subaccount.get_required_datetimerange()
            self.subaccount.dataprovider.load_datasets_to_memory(range)
            self._set_current_candle_timestamp()

    def _load_new_candles(self) -> None:
        """
        Periodically check based on the UTC timestamp and the timeframe if a new candle can be loaded from the
        exchange. If a new candle exists, load it to the cache and data. If not, wait until a new candle exists.
        This function blocks the execution of the process.
        """
        if not self.subaccount.is_backtest():
            while True:
                if datetime.datetime.utcnow().timestamp() > self.current_candle_timestamp:  #
                    self._load_candles()
                else:
                    time.sleep(1)

    def _set_current_candle_timestamp(self) -> None:
        """
        After loading the dataset get the latest timestamp. This timestamp is used to check
        if a new candle is available. Ideally (TIMEFRAME *  60) seconds there is a new candle available from the API.
        """
        df = self._get_main_df()
        self.current_candle_timestamp = df["date"].iloc[-1].timestamp()

    def _get_main_df(self) -> DataFrame:
        """
        Return the dataframe of the main (trading) pair from the dataprovider.
        :return: dataframe
        """
        return self.subaccount.dataprovider.get_pair_dataframe(self.subaccount.dataprovider.main_pair)

    def _get_df_length(self) -> int:
        """
        Return the length of the main pair dataframe.
        :return: length of dataframe
        """
        return len(self._get_main_df().index)

    def _get_index(self) -> int:
        """
        Return the index that should be used for the current tick call.
        In backtest mode the current position in the dataframe will be used.
        In live mode the last candle will be used
        :return: integer position in dataframe
        """
        if self.subaccount.is_backtest():
            return self.df_position
        else:
            return self._get_df_length() - 1

    def _active(self) -> bool:
        """
        Check if the main loop should continue running.
        In backtest mode check if the dataframe position is out of bounds.
        In live mode check if there is still money on the wallet.
        :return: true if the main loop should continue, false if not
        """
        if self.subaccount.is_backtest():
            return self.df_position < self._get_df_length()
        else:
            return True

    def _init_exchange(self):
        """
        Perform necassary steps to ensure the exchange is ready for operation.
        In backtest mode set the dataframe.
        :return:
        """
        if self.subaccount.is_backtest() and self._get_index() == 0:
            self.subaccount.exchange.set_dataframe(self._get_main_df())
        self.subaccount.exchange.subaccount_id = self.subaccount_id
        self.subaccount.exchange.optimize_id = self.optimize_id
        self.subaccount.exchange.pair_id = self.pair_id
        self.subaccount.exchange.init_exchange()

    def _init_progress(self) -> None:
        """
        Initialize a tqdm progress bar for backtest mode.
        """
        if False and self.subaccount.is_backtest():
            self.progress_bar = tqdm.tqdm(total=self._get_df_length())

    def _progress_step(self) -> None:
        """
        Continue the tqdm progress bar a single step.
        :return:
        """
        if False and self.subaccount.is_backtest():
            self.progress_bar.update(1)

    def _free_progress(self) -> None:
        """
        Finish a tqdm progress bar.
        :return:
        """
        if False and self.subaccount.is_backtest():
            time.sleep(1)
            self.progress_bar.close()

    def _populate_indicators(self, subaccount: SubaccountItem,
                             df: DataFrame,
                             metadata: Dict[str, Any],
                             parameters: Dict[str, List[Any]]):
        """
        Populate the indicators.
        In backtest mode calculate the indicators only once since the dataframe is complete from the start.
        In live mode calculate the indicators every time since there is a new candle at the end.
        :param subaccount: subaccount
        :param df: dataframe
        :param metadata: metadata
        :param parameters: parameters
        :return: dataframe with indicators
        """

        if self.recalculate_inidcators:
            df = subaccount.strategy.populate_indicators(dataframe=df, metadata=metadata, parameters=parameters)
            utils.create_missing_columns(self.subaccount.run_settings.db_path, "ticker", df)
            df["subaccount_id"] = self.subaccount_id
            df["pair_id"] = self.pair_id
            df["optimize_id"] = self.optimize_id
            df.to_sql(name="ticker", con=get_engine(self.subaccount.run_settings.db_path), if_exists='append')
            self.recalculate_inidcators = False

        return df

    def _plot_subaccount(self):
        """
        Plot the indicators, order, exectuions and wallet of the current subaccount.
        """
        if not self.subaccount.is_optimization():
            plotter = PlotterSubaccount()
            plotter.plot_subaccount_range(
                self.subaccount.run_settings.db_path,
                Path(os.path.join(
                    self.subaccount.run_settings.run_dir,
                    self.subaccount.subaccount_config["subaccount_id"] + ".html"
                )),
                self.subaccount.subaccount_config["subaccount_id"],
                self.subaccount_id,
                datetime.datetime(year=1900, month=1, day=1),
                datetime.datetime.utcnow(),
                self.subaccount.strategy.get_indicators()
            )

    def _init_optimize(self):
        """
        Create the optimizer.
        """
        if not self.subaccount.is_optimization():
            self.optimizer = Optimizer(self.subaccount)

    def _optimize_start(self):
        """
        Start the optimization process.
        Check if a optimization is necassary. Then create the in-sample period and a backtest each possible parameter
        combination in that period. Use the best parameter for the out-of-sample period.
        :return:
        """
        if self._optimization_necassary():
            df = self._get_main_df()
            index = self._get_index()
            date = utils.pdts_to_pydt(df.at[index, "date"])

            train_period = DatetimePeriod(
                start=date - datetime.timedelta(days=self.subaccount.config["optimization"]["days_train"]),
                end=date,
            )
            test_period = DatetimePeriod(
                start=date,
                end=date + datetime.timedelta(days=self.subaccount.config["optimization"]["days_test"]),
            )
            if not self.optimizer.running:
                if self.subaccount.is_backtest():
                    self.optimizer.start(train_period, test_period, callback=self._optimize_finished)
                else:
                    self.optimizer.start(train_period, test_period, callback=self._optimize_finished)

    def _optimization_necassary(self) -> bool:
        """
        Check if an optimization process is necassary.
        Every day at midnight check if the current parametes are still valid.
        The days_test parameter from the config indicates how long the parameters for out-of-sample execution is valid.
        :return: true if optimization is necassary
        """
        df = self._get_main_df()
        index = self._get_index()
        date = utils.pdts_to_pydt(df.at[index, "date"])

        if not self.subaccount.is_optimization():
            if index > 1 and date.day != utils.pdts_to_pydt(df.at[index - 1, "date"]):
                if date >= self.optimizer.test_period.end:
                    return True
        return False

    def _optimize_finished(self, params):
        """
        Callback function for optimizer. When optimizer finishes the walk-forward optimization, this function
        saves the best parameter.
        """
        self.optimized_parameter = params
        self.recalculate_inidcators = True

    def _optimize_get_parameter(self) -> Dict[str, Any]:
        """

        :return:
        """
        if not self.subaccount.is_optimization():
            return self.optimized_parameter
        else:
            return self.subaccount.parameter

    def _optimize_get_parameters_indicators(self) -> Dict[str, List[Any]]:
        if not self.subaccount.is_optimization():
            return self.subaccount.strategy.populate_parameters()
        else:
            parameter = copy.copy(self.subaccount.parameter)
            for key, value in parameter.items():
                parameter[key] = [value]
            return parameter


