import datetime
import os
from pathlib import Path
from shutil import copyfile
from typing import Dict, Any
import pandas as pd
import pytz
import unicodedata
import re

def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')

def get_history_dir(config: Dict[str, Any]) -> Path:
    """
    Return the directory where all the runs are stored.
    :param config: config file
    :return: path to folder
    """
    folder = config["history_data_dir"]
    return folder

def get_run_history_dir(config: Dict[str, Any], run_id: str) -> Path:
    """
    Return the directory where the current run is stored.
    :param config: config file
    :param run_id: run id
    :return: path to folder
    """
    folder = Path(get_history_dir(config), run_id)
    return folder

def copy_file_to_folder(file_path: Path, folder_path: Path) -> None:
    """
    Copy file to folder and keep same name.
    :param file_path: path to file
    :param folder_path: path to folder
    :return: None
    """
    copyfile(file_path, os.path.join(folder_path, os.path.basename(file_path)))


def parse_datetime_string(datetime_string: str) -> datetime.datetime:
    """
    Parse datetime in "31.01.2021 00:00:00" format.
    :param datetime_string: string representation
    :return: datetime object
    """
    if " " in datetime_string:
        return datetime.datetime.strptime(datetime_string, '%d.%m.%Y %H:%M:%S').replace(tzinfo=pytz.utc)
    else:
        return datetime.datetime.strptime(datetime_string, '%d.%m.%Y').replace(tzinfo=pytz.utc)


def sanitize_pair(pair: str) -> str:
    """
    Remove seperation symbols from pair string.
    :param pair: pair name (BTC/USDT)
    :return: pair without seperation symbols (BTCUSDT)
    """
    return pair.replace("/", "").replace("\\", "").replace("-", "").replace("_", "")


def timeframe_int_to_str(timeframe: int) -> str:
    """
    Convert timeframe from integer to string
    :param timeframe: minutes per candle (240)
    :return: string representation for API (4h)
    """
    if timeframe < 60:
        return f"{timeframe}m"
    elif timeframe < 1440:
        return f"{int(timeframe / 60)}h"
    else:
        return f"{int(timeframe / 1440)}d"

def timeframe_str_to_int(timeframe: str) -> int:
    """
    Convert timeframe from string to integer
    :param timeframe: string representation from API (4h)
    :return: minutes per candle (240)
    """
    if "m" in timeframe:
        return int(timeframe.replace("m", ""))
    elif "h" in timeframe:
        return int(timeframe.replace("h", "")) * 60
    elif "d" in timeframe:
        return int(timeframe.replace("d", "")) * 1440
    else:
        raise Exception("Unsupported timeframe")

def unix_to_pdts(unix: int) -> pd.Timestamp:
    """
    Convert unix timestamp (seconds) to pandas timestamp
    """
    return pd.Timestamp(unix, unit='s', tz='UTC')

def pydt_to_pdts(pydt: datetime.datetime) -> pd.Timestamp:
    """
    Covert python datetime to pandas timestamp
    """
    return pd.Timestamp(pydt, unit='s', tz='UTC')

def pdts_to_pydt(pdts: pd.Timestamp) -> datetime.datetime:
    """
    Convert pandas timestamp to python datetime.
    """
    return pdts.to_pydatetime()

def create_missing_columns(db_path: Path, table: str, df: pd.DataFrame) -> None:
    """
    Add columns of dataframe to table in database.
    :param db_path: path to sqlite db
    :param table: table name
    :param df: pandas dataframe
    """
    from kektrade.database.types import get_engine
    engine = get_engine(db_path)
    with engine.connect() as con:
        for column in list(df.columns):
            try:
                statement = f"alter table {table} add column {column}"
                con.execute(statement)
            except:
                pass