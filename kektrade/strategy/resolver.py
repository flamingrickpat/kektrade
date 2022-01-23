import glob
import importlib.machinery
import importlib.util
import inspect
import logging
from pathlib import Path
from typing import List, Tuple, Union

from kektrade.exceptions import StrategyNotFoundException, StrategyDefinedMultipleTimes
from kektrade.strategy import IStrategy

logger = logging.getLogger(__name__)

class StrategyResolver():
    @classmethod
    def load_strategy(cls, search_path: str, class_name: str) -> Tuple[Path, IStrategy]:
        """
        Find and load a custom class in a specified directory recursively. Raises an error if a multiple classes or no
        classes with specified name are found.
        :param search_path: folder that contains custom classes or subfolders with custom classes
        :param strategy_name: class name of custom strategy
        :return: tuple with path to module and instanced class
        """
        modules: List[str] = StrategyResolver._get_modules_recursive(search_path)
        strategies: List[Tuple[Path, IStrategy]] = []

        for module_path in modules:
            strategy = StrategyResolver._load_class(module_path, class_name)
            if strategy is not None:
                strategies.append((Path(module_path), strategy))

        if len(strategies) == 0:
            logger.error(f"Strategy with class name {class_name} was not found.")
            raise StrategyNotFoundException()
        elif len(strategies) > 1:
            logger.error(f"Strategy with class name {class_name} is defined multiple times:")
            for (strategy, path) in strategies:
                logger.error(f"{path}")
            raise StrategyDefinedMultipleTimes()
        else:
            return strategies[0]


    @staticmethod
    def _get_modules_recursive(search_path: str) -> List[str]:
        """
        Find all .py modules recursively in directory.
        :param search_path: directory path
        :return: relative paths of all .py files in folder structure
        """
        return glob.glob(search_path + '/**/*.py', recursive=True)


    @staticmethod
    def _load_module(module_path: str):
        """
        Dynamically load module from file path.
        :param module_path: path to module file
        :return: module object
        """
        loader = importlib.machinery.SourceFileLoader("", module_path)
        spec = importlib.util.spec_from_loader(loader.name, loader)
        mod = importlib.util.module_from_spec(spec)
        loader.exec_module(mod)
        return mod


    @staticmethod
    def _load_class(module_path: str, class_name: str) -> Union[None, IStrategy]:
        """
        Dynamically load module and check if specified class exists in module members.
        If it does, return a new instance. If not return None.
        :param module_path: path to module file
        :param class_name: name of custom class
        :return: instance of custom class or None
        """
        mod = StrategyResolver._load_module(module_path)
        members = inspect.getmembers(mod)
        strategy: Union[None, IStrategy] = None
        for (name, classtype) in members:
            if name == class_name:
                strategy = classtype()
        return strategy