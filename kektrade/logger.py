import logging
import sys
from enum import Enum
from functools import wraps
from typing import Dict, Any

from kektrade.exceptions import UnsupportedLoggingLevel

logger = logging.getLogger(__name__)

LOG_FORMAT = '%(asctime)s - [%(thread)d] - %(name)s - %(levelname)s - %(message)s'

class LoggingLevel(Enum):
    Debug = "debug"
    Info = "info"
    Warning = "warning"
    Error = "error"
    Critical = "critical"

def parse_config_log_level(level: str) -> int:
    """
    Takes the string from the config and converts it to a integer for the logging module level.
    :param level: logging level from enum LoggingLevel
    :return: logging level from logging consts
    """
    if level == LoggingLevel.Debug.value:
        return logging.DEBUG
    elif level == LoggingLevel.Info.value:
        return logging.INFO
    elif level == LoggingLevel.Warning.value:
        return logging.WARNING
    elif level == LoggingLevel.Error.value:
        return logging.ERROR
    elif level == LoggingLevel.Critical.value:
        return logging.CRITICAL
    else:
        raise UnsupportedLoggingLevel()

def alogger(fn) -> None:
    """
    Annotation to quickly measure the runtime of specific functions.
    :param fn: Function to Wrap with "Enter" and "Exit" log lines.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        log = logging.getLogger(fn.__name__)
        log.info('ENTER %s' % fn.__name__)

        out = fn(*args, **kwargs)

        log.info('EXIT  %s' % fn.__name__)
        return out
    return wrapper


def setup_logging_default() -> None:
    """
    The default logger before a config file is read. Logs everything to stdout.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    root.addHandler(handler)


def setup_logging_config(config: Dict[str, Any], filename: str) -> None:
    """
    Sets up the logger once the config has been parsed. This should be called once per process.
    There is no rotating file handler, the file grows forever.
    :param config: Dictionary with configuration
    :param filename: Path to file on disk
    :return:
    """
    root = logging.getLogger()
    root.handlers = []
    formatter = logging.Formatter('[%(asctime)s - [%(process)d] - %(name)s - %(funcName)s - %(levelname)s] %(message)s')

    level = parse_config_log_level(config["log_level"])

    if config["log_console"]:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        handler.setLevel(level)
        root.addHandler(handler)

    if filename is not None:
        fileHandler = logging.FileHandler(filename)
        fileHandler.setFormatter(formatter)
        fileHandler.setLevel(level)
        root.addHandler(fileHandler)

    root.setLevel(level)