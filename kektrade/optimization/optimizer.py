import copy
from itertools import product
from multiprocessing import Pool
from pathlib import Path
import logging
import pytz
from typing import Union

from sqlalchemy.sql import select
import pandas as pd
import tabulate

from kektrade.event_loop import *
from kektrade.subaccount import SubaccountItem
from kektrade.data.dataprovider import DatetimePeriod
from kektrade.exchange import Backtest
from kektrade.database.types import Subaccount, Pair, get_engine
from kektrade.database.types import get_session
from kektrade.plotting import PlotterSubaccount
from kektrade.subaccount import SubaccountItem
from kektrade.config.runtime_settings import RunSettings
from kektrade.plotting import plotter_optimize
from kektrade.database.types import *

logger = logging.getLogger(__name__)

class Optimizer():
    """
    Walk-forward optimization.
    """

    def __init__(self, subaccount: SubaccountItem):
        """
        Set the template subaccount to run the optimization process on.
        In Live mode set a backtest backend.
        :param subaccount:
        """
        subaccount_template = copy.copy(subaccount)
        subaccount_template.run_settings = RunSettings(
            run_id=subaccount_template.run_settings.run_id,
            run_dir=subaccount_template.run_settings.run_dir,
            run_continue=subaccount_template.run_settings.run_continue,
            db_path=subaccount_template.run_settings.db_path
        )
        subaccount_template.load_modules()
        if isinstance(subaccount.exchange, Backtest) or issubclass(subaccount.exchange.__class__, Backtest):
            pass
        else:
            raise Exception("not implemented")

        self.subaccount_template: SubaccountItem = subaccount_template
        self.running: bool = False

        tmp = datetime.datetime(year=1900, month=1, day=1, tzinfo=pytz.utc)
        self.train_period: DatetimePeriod = DatetimePeriod(start=tmp, end=tmp)
        self.test_period: DatetimePeriod = DatetimePeriod(start=tmp, end=tmp)

    def start(self, train_period, test_period: DatetimePeriod, callback):
        self.train_period = train_period
        self.test_period = test_period

        if self.subaccount_template.config["optimization"]["log_optimization"]:
            logger.info("")
            logger.info(f"=== Start optimization ===")
            logger.info(f"Train period: {train_period}")
            logger.info(f"Test period:  {test_period}")

        optimizeable_parameter = self._get_optimizeable_parameter()
        parameter_combinations = self._get_parameter_product(optimizeable_parameter)
        parameter_combinations = self._get_parameters_with_default_values(parameter_combinations)

        subaccount_configurations = []
        for parameter in parameter_combinations:
            subaccount = copy.copy(self.subaccount_template)
            subaccount.start = train_period.start
            subaccount.end = train_period.end
            subaccount.parameter = parameter
            subaccount.parent_subaccount_id = self.subaccount_template.id
            subaccount_configurations.append(subaccount)

        if self.subaccount_template.config["optimization"]["log_optimization"]:
            logger.info(f"Possible parameter combinations: {parameter_combinations}")

        subaccount_ids = []
        from kektrade.event_loop import start_eventloop
        cpu_count = 1
        if cpu_count == 1:
            for sa in subaccount_configurations:
                subaccount_ids.append(start_eventloop(sa))
        else:
            pool = Pool(16)
            subaccount_ids = pool.map(start_eventloop, subaccount_configurations)
            pool.join()

        best_parameter = self._get_best_parameter(subaccount_ids)

        if self.subaccount_template.config["optimization"]["plot_optimization"]:
            self._plot_optimization(train_period, subaccount_ids)

        if self.subaccount_template.config["optimization"]["log_optimization"]:
            logger.info(f"Best parameter combination: {best_parameter}")
            logger.info(f"=== Finish optimization ===")
            logger.info("")

        callback(best_parameter)


    def _get_optimizeable_parameter(self) -> Dict[Any, List[Any]]:
        optimizeable_parameter = self.subaccount_template.strategy.populate_parameters()
        for key, value in self.subaccount_template.subaccount_config["parameters"].items():
            del optimizeable_parameter[key]
        return optimizeable_parameter

    def _get_parameter_product(self, parameter: Dict[str, List[Any]]) -> List[Dict[str, List[Any]]]:
        generator = (dict(zip(parameter.keys(), values)) for values in product(*parameter.values()))
        return list(generator)

    def _get_parameters_with_default_values(self, parameters: List[Dict[str, List[Any]]]) -> List[Dict[str, List[Any]]]:
        result = []
        for parameter in parameters:
            for key, value in self.subaccount_template.subaccount_config["parameters"].items():
                parameter[key] = value
            result.append(parameter)
        return result

    def _plot_optimization(self, period: DatetimePeriod, subaccount_ids: List[int]):
        plotter = plotter_optimize.PlotterOptimize()
        plotter.plot_optimize(
            self.subaccount_template.run_settings.db_path,
            Path(os.path.join(
                self.subaccount_template.run_settings.run_dir,
                self.subaccount_template.subaccount_config["subaccount_id"] +
                f"_OPTIMIZE_{period.start.strftime('%Y%m%d')}_{period.end.strftime('%Y%m%d')}.html",
            )),
            f"Optimization for {period}",
            subaccount_ids
        )

    def _get_best_parameter(self, subaccount_ids: List[int]) -> Union[None, Dict[str, Any]]:
        """
        Get the data of the specified subaccounts from the db, calculate metric and return the best one.
        :param subaccount_ids: list of the subaccount database ids
        :return: Dictionary with best parameters or None if all fail
        """
        session = get_session(self.subaccount_template.run_settings.db_path)
        conn = session.bind

        query = select(Subaccount).filter(Subaccount.id.in_(subaccount_ids))
        data_subaccounts = pd.read_sql(query, con=conn)

        query = select(Wallet).filter(Wallet.subaccount_id.in_(subaccount_ids))
        data_wallet = pd.read_sql(query, con=conn)

        parameter_metric = {}
        data_subaccounts["metric"] = 0.0
        for i in range(len(data_subaccounts.index)):
            subaccount_id = data_subaccounts.at[i, "id"]
            df_tmp = data_wallet[data_wallet["subaccount_id"] == subaccount_id]
            parameter_metric[subaccount_id] = df_tmp["account_balance"].iloc[-1]
            data_subaccounts.at[i, "metric"] = parameter_metric[subaccount_id]

        best_id = max(parameter_metric, key=parameter_metric.get)
        best_param = data_subaccounts[data_subaccounts["id"] == best_id].reset_index().at[0, "parameter"]
        if self.subaccount_template.config["optimization"]["log_optimization"]:
            logger.info("\n" + tabulate.tabulate(data_subaccounts, headers='keys', tablefmt='psql'))

        import ast
        return ast.literal_eval(best_param)


