
"""Pathlib-like generic abstraction.
adapted from https://github.com/tensorflow/datasets/blob/v4.4.0/tensorflow_datasets/core/utils/generic_path.py
"""


import os
import typing
from typing import Callable, Dict, Tuple, Type, Union, TypeVar, List

from fileio.core import iopath
from fileio.core import type_utils
from fileio.core.libs import GCS_METHOD

PathLike = type_utils.PathLike
ReadOnlyPath = type_utils.ReadOnlyPath
ReadWritePath = type_utils.ReadWritePath

PathLikeCls = Union[Type[ReadOnlyPath], Type[ReadWritePath]]
T = TypeVar('T')

_PATHLIKE_CLS: Tuple[PathLikeCls, ...] = (
    iopath.PosixGCSPath,
    iopath.PosixS3Path,
    iopath.PosixIOPath,
    iopath.WindowsGPath,
)
_URI_PREFIXES_TO_CLS: Dict[str, PathLikeCls] = {
    # Even on Windows, `gs://`,... are PosixPath
    'gs://': iopath.PosixIOPath if GCS_METHOD == 'tf' else iopath.PosixGCSPath,
    's3://': iopath.PosixIOPath if GCS_METHOD == 'tf' else iopath.PosixGCSPath,
}


@typing.overload
def register_pathlike_cls(path_cls_or_uri_prefix: str) -> Callable[[T], T]:
    ...


@typing.overload
def register_pathlike_cls(path_cls_or_uri_prefix: T) -> T:
    ...


def register_pathlike_cls(path_cls_or_uri_prefix):
    """Register the class to be forwarded as-is in `as_path`.
    ```python
    @utils.register_pathlike_cls('my_path://')
    class MyPath(pathlib.PurePosixPath):
        ...
    my_path = tfds.core.as_path('my_path://some-path')
    ```
    Args:
        path_cls_or_uri_prefix: If a uri prefix is given, then passing calling
            `tfds.core.as_path('prefix://path')` will call the decorated class.
    Returns:
        The decorator or decoratorated class
    """
    global _PATHLIKE_CLS
    if isinstance(path_cls_or_uri_prefix, str):

        def register_pathlike_decorator(cls: T) -> T:
            _URI_PREFIXES_TO_CLS[path_cls_or_uri_prefix] = cls
            return register_pathlike_cls(cls)

        return register_pathlike_decorator
    else:
        _PATHLIKE_CLS = _PATHLIKE_CLS + (path_cls_or_uri_prefix,)
        return path_cls_or_uri_prefix


def as_path(path: PathLike) -> ReadWritePath:
    """Create a generic `pathlib.Path`-like abstraction.
    Depending on the input (e.g. `gs://`, `github://`, `ResourcePath`,...), the
    system (Windows, Linux,...), the function will create the right pathlib-like
    abstraction.
    Args:
        path: Pathlike object.
    Returns:
        path: The `pathlib.Path`-like abstraction.
    """
    is_windows = os.name == 'nt'
    if isinstance(path, str):
        uri_splits = path.split('://', maxsplit=1)
        if len(uri_splits) > 1:    # str is URI (e.g. `gs://`, `github://`,...)
            # On windows, `PosixGPath` is created for `gs://` paths
            return _URI_PREFIXES_TO_CLS[uri_splits[0] + '://'](path)
        elif is_windows:
            return iopath.WindowsGPath(path)
        else:
            return iopath.PosixIOPath(path)
    elif isinstance(path, _PATHLIKE_CLS):
        return path
    elif isinstance(path, os.PathLike):    # Other `os.fspath` compatible objects
        path_cls = iopath.WindowsGPath if is_windows else iopath.PosixIOPath
        return path_cls(path)
    else:
        raise TypeError(f'Invalid path type: {path!r}')


def get_pathlike(filepath: Union[str, PathLike]):
    if isinstance(filepath, str): filepath = as_path(filepath)
    return filepath

def filter_files(files: List[Union[str, PathLike]], include=[], exclude=['.git']):
    files = [get_pathlike(f) for f in files]
    if include: files = [f for f in files if bool(set(f.parts).intersection(include))]
    if exclude: files = [f for f in files if not bool(set(f.parts).intersection(exclude))]
    return files