import logging
import os
from multiprocessing import Lock
from typing import Any, List, Dict
from multiprocessing import Pool, freeze_support, Manager

from kektrade import utils
from kektrade.config import RunSettings
from kektrade.data import *
from kektrade.event_loop import start_eventloop
from kektrade.exchange import ExchangeResolver
from kektrade.strategy import StrategyResolver
from kektrade.database.types import get_session
from kektrade.exchange import IExchange
from kektrade.strategy import IStrategy
from kektrade.subaccount import SubaccountItem

logger = logging.getLogger(__name__)

class KektradeBot():
    """
    The KektradeBot class sets up and manages the subaccount processes.
    It receives commands from the api and handles the plotting and reporting.
    """

    def __init__(self, config: Dict[str, Any], run_settings: RunSettings):
        self.config: Dict[str, Any] = config
        self.run_settings: RunSettings = run_settings

        self.subaccounts: List[SubaccountItem] = []

        m = Manager()
        self.file_lock = m.Lock()


    def setup_database(self) -> None:
        """
        Create the sqlite database if it does not exist yet.
        """
        logger.info("Setting up database")
        get_session(self.run_settings.db_path)

    def setup_subaccounts(self) -> None:
        """
        Go over the subaccount entries in the config.
        For every subaccount copy the strategy source to the history directory.
        Put all subaccount objects into a list for later processing.
        """

        for subaccount in self.config["subaccounts"]:
            (module_path, _) = StrategyResolver.load_strategy(
                search_path=os.path.join(self.config["user_data_dir"], self.config["strategy_data_dir"]),
                class_name=subaccount["strategy"]
            )
            utils.copy_file_to_folder(module_path, self.run_settings.run_dir)

            start = utils.parse_datetime_string(self.config["backtest_start"])
            end = utils.parse_datetime_string(self.config["backtest_end"])

            sa = SubaccountItem(self.config, subaccount, self.run_settings, self.file_lock, start, end)
            self.subaccounts.append(sa)


    def setup_plotter(self):
        pass


    def setup_api(self):
        pass


    def start(self) -> None:
        """
        Start the main event loop and provide the subaccoutn objects.
        In live mode each subaccount spwans a process and they run in paralell.
        In backtest mode the subaccounts are run one after another.
        """
        #pool = Pool(1)
        #pool.map(start_eventloop, self.subaccounts)
        for subaccount in self.subaccounts:
            start_eventloop(subaccount)
