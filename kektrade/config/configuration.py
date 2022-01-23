import json
import os
from typing import Dict, Any

from kektrade.config.validator import validate_config
from kektrade.exceptions import ConfigNotFoundException


def get_config(path: str) -> Dict[str, Any]:
    """
    Read config file and validate it.
    :param path: path on file system
    :return: validated dict with default params
    """
    config = read_configuration_from_file(path)
    config = validate_config(config)
    return config


def read_configuration_from_file(path: str) -> Dict[str, Any]:
    """
    Read the JSON file and return a dict.
    :param path: path on file system
    :return: raw, unchanged dict
    """
    if os.path.isfile(path):
        with open(path) as json_file:
            return json.load(json_file)
    else:
        raise ConfigNotFoundException