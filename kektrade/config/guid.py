import datetime
import random
import string


def generate_guid(postfix=None) -> str:
    """
    Generate a unique identifier for a run. The GUID will be used to create a directory with the database,
    log files and plot files.
    :param postfix: Optional name for easy readability. Will default to string with 5 letters.
    :return: String with date and identifier
    """
    if postfix is None:
        postfix = ''.join(random.choice(string.ascii_lowercase) for x in range(5))
    return datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S_" + postfix)