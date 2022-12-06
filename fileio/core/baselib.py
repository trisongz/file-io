"""
Handles Importing the correct pathlib for this library in
python3.10 cases
"""

import sys

# if 3.10
if sys.version_info.minor >= 10:
    from fileio.core import _pathlib as pathlib
    from fileio.core._pathlib import Path
else:
    import pathlib
    from pathlib import Path


__all__ = ('pathlib', 'Path')