from pathlib import Path
from typing import NamedTuple

class RunSettings(NamedTuple):
    """
    Data container to store variables that will later be passed on to child processes.
    """
    run_id: str
    run_dir: Path
    run_continue: bool
    db_path: Path