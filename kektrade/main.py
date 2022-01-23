import logging
import os
from typing import List
from pathlib import Path

from kektrade import utils
from kektrade.config import RunSettings
from kektrade.config import generate_guid
from kektrade.config import get_config
from kektrade.config import validate_arguments
from kektrade.exceptions import InvalidRunIdException
from kektrade.kektradebot import KektradeBot
from kektrade.logger import setup_logging_default, setup_logging_config

logger = logging.getLogger('freqtrade')

def main(args: List[str]) -> None:
    """
    Parse the arguments and config file.
    Setup the logger.
    :param args: parameters
    """
    setup_logging_default()

    args = validate_arguments(args)

    config_path = args.config
    config = get_config(config_path)

    if args.run_id is not None:
        run_continue = True

        run_id = args.run_id
        folder = utils.get_run_history_dir(config, run_id)
        if not folder.is_dir():
            raise InvalidRunIdException
    else:
        run_continue = False
        run_id = generate_guid(config["metastrategy_id"])

    run_dir = utils.get_run_history_dir(config, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    setup_logging_config(config, os.path.join(run_dir, "main.log"))

    db_path = Path(os.path.join(run_dir, config["metastrategy_id"] + ".db"))

    if run_continue:
        logger.info(f"Continuing kektrade with existing RunID: {run_id}")
    else:
        logger.info(f"Starting kektrade with RunID: {run_id}")

    run_settings = RunSettings(
        run_id=run_id,
        run_continue=run_continue,
        run_dir=run_dir,
        db_path=db_path
    )
    utils.copy_file_to_folder(config_path, run_settings.run_dir)

    run = KektradeBot(config, run_settings)
    run.setup_database()
    run.setup_subaccounts()
    run.setup_plotter()
    run.setup_api()
    run.start()











