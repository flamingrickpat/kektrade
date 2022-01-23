class UnsupportedLoggingLevel(Exception):
    """
    Unknown logging level in config file.
    """

class ConfigNotFoundException(Exception):
    """
    Path to config file is invalid.
    """

class InvalidRunIdException(Exception):
    """
    RunID is specified in arguments, but doesn't exist in history dir.
    """

class StrategyNotFoundException(Exception):
    """
    The strategy was not found in the specified folder or its subfolders.
    """

class StrategyDefinedMultipleTimes(Exception):
    """
    There are multiple classes with the same name in the specified folder or subfolders.
    """

class UnsupportedExchange(Exception):
    """
    Unknown exchange provided.
    """

class UnknownExchangeParameter(Exception):
    """
    Exchange parameter from config is not compatible with exchange class.
    """

class ExchangeException(Exception):
    """
    Exception occuring inside exchange class.
    """