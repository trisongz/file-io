import os
from typing import List, Optional
from fileio.core import type_utils
from fileio.configs import DATA_DIR

_registered_data_dir = set()


def add_data_dir(data_dir):
    """Registers a new default `data_dir` to search for datasets.
    When a `tfds.core.DatasetBuilder` is created with `data_dir=None`, TFDS
    will look in all registered `data_dir` (including the default one) to
    load existing datasets.
    * An error is raised if a dataset can be loaded from more than 1 registered
        data_dir.
    * This only affects reading datasets. Generation always uses the
        `data_dir` kwargs when specified or `tfds.core.constant.DATA_DIR` otherwise.
    Args:
        data_dir: New data_dir to register.
    """
    # Remove trailing / to avoid same directory being included twice in the set
    # with and without a final slash.
    data_dir = data_dir.rstrip('/')
    _registered_data_dir.add(data_dir)


def list_data_dirs(given_data_dir: Optional[str] = None,) -> List[str]:
    """Return the list of all `data_dir` to look-up.
    Args:
        given_data_dir: If a `data_dir` is provided, only the explicitly given
            `data_dir` will be returned, otherwise the list of all registered data_dir
            is returned
    Returns:
        The list of all data_dirs to look-up.
    """
    # If the data dir is explicitly given, no need to search everywhere.
    if given_data_dir: return [given_data_dir]
    all_data_dirs = _registered_data_dir | {DATA_DIR}
    return sorted(os.path.expanduser(d) for d in all_data_dirs)


def get_default_data_dir(given_data_dir: Optional[str] = None,) -> str:
    """Returns the default data_dir."""
    if given_data_dir: return os.path.expanduser(given_data_dir)
    return os.path.expanduser(DATA_DIR)