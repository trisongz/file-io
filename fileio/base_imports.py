"""
Handles Importing the correct pathlib for this library in
python3.10 cases
"""

import sys
import os
import io
import inspect
import ntpath
import posixpath

from anyio import open_file, AsyncFile
from os import stat_result, PathLike
from contextlib import asynccontextmanager

from typing import TYPE_CHECKING, Union
from typing import Optional, List, AsyncIterable, Iterable, IO, AsyncContextManager, cast, Callable
from .aiopath.wrap import coro_as_method_coro, func_as_method_coro, to_thread, method_as_method_coro, func_to_async_func
from .aiopath.handle import IterableAIOFile, get_handle
from .aiopath.types import Final, Literal, FileMode


# if 3.10
if sys.version_info.minor >= 10:
    from . import _pathlib as pathlib
    from ._pathlib import PosixPath, WindowsPath, Path, PurePath, _ignore_error
    from ._pathlib import _NormalAccessor as NormalAccessor
    from ._pathlib import _make_selector as _sync_make_selector
    from ._pathlib import _PosixFlavour, _WindowsFlavour
    try:
        from ._pathlib import _getfinalpathname
        _async_getfinalpathname = func_to_async_func(_getfinalpathname)

    except ImportError:
        def _getfinalpathname(*args, **kwargs):
            raise ImportError("_getfinalpathname() requires a Windows/NT platform")

        async def _async_getfinalpathname(*args, **kwargs):
            raise ImportError("_getfinalpathname() requires a Windows/NT platform")
        
else:
    import pathlib
    from pathlib import PosixPath, WindowsPath, Path, PurePath, _ignore_error
    from pathlib import _NormalAccessor as NormalAccessor
    from pathlib import _make_selector as _sync_make_selector
    from pathlib import _PosixFlavour, _WindowsFlavour
    try:
        from pathlib import _getfinalpathname
        _async_getfinalpathname = func_to_async_func(_getfinalpathname)

    except ImportError:
        def _getfinalpathname(*args, **kwargs):
            raise ImportError("_getfinalpathname() requires a Windows/NT platform")
    
        async def _async_getfinalpathname(*args, **kwargs):
            raise ImportError("_getfinalpathname() requires a Windows/NT platform")



DEFAULT_ENCODING: Final[str] = 'utf-8'
ON_ERRORS: Final[str] = 'ignore'
NEWLINE: Final[str] = '\n'

BEGINNING: Final[int] = 0
CHUNK_SIZE: Final[int] = 4 * 1_024

SEP: Final[str] = '\n'
ENCODING: Final[str] = 'utf-8'
ERRORS: Final[str] = 'replace'

FileData = Union[bytes, str]
Paths = Union[Path, PathLike, str]
Handle = AsyncFile


def iscoroutinefunction(obj):
    if inspect.iscoroutinefunction(obj): return True
    if hasattr(obj, '__call__') and inspect.iscoroutinefunction(obj.__call__): return True
    return False


__all__ = (
    'io',
    'os',
    'inspect',
    'ntpath',
    'posixpath',
    'open_file',
    'AsyncFile',
    'stat_result',
    'PathLike',
    'asynccontextmanager',
    'pathlib', 
    'PosixPath',
    'WindowsPath',
    'PurePath',
    'Path',
    '_ignore_error',
    '_PosixFlavour',
    '_WindowsFlavour',
    'NormalAccessor',
    '_sync_make_selector',
    '_getfinalpathname',
    '_async_getfinalpathname',
    'coro_as_method_coro',
    'func_as_method_coro',
    'to_thread',
    'method_as_method_coro',
    'func_to_async_func',
    'IterableAIOFile',
    'Final',
    'Literal',
    'get_handle',
    'FileMode',
    'DEFAULT_ENCODING',
    'ON_ERRORS',
    'NEWLINE',
    'TYPE_CHECKING',
    'BEGINNING',
    'CHUNK_SIZE',
    'SEP',
    'ENCODING',
    'ERRORS',
    'FileData', 'Paths', 'Handle',
    'iscoroutinefunction',
    'Union', 'Optional', 'List', 'AsyncIterable', 'Iterable', 'IO', 'AsyncContextManager', 'cast', 'Callable'
)