import copy
import logging
from functools import cmp_to_key

import numpy as np
import pandas as pd
from pandas import DataFrame

from kektrade.exchange.interface import *
from kektrade.exchange.backtest import Backtest

logger = logging.getLogger(__name__)

class BacktestLinear(Backtest):
    pass
