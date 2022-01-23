import argparse
from typing import List

def validate_arguments(args: List[str]) -> argparse.Namespace:
    """
    Parse the command line arguments and check for validity.
    :param args: argument list
    :return: namespace with valid values.
    """
    parser = argparse.ArgumentParser(description='execute kektrade trading bot')

    parser.add_argument('config', metavar="CONFIG", type=str, help='path to config file')
    parser.add_argument('--run_id', type=str, help='run id of a previous run that should be continued')

    return parser.parse_args(args=args)