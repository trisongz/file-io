
"""
Adapted from https://github.com/tensorflow/datasets/blob/v4.4.0/tensorflow_datasets/core/utils/gpath.py
"""

import ntpath
import os
import pathlib
import posixpath
import types
import typing
from typing import Any, ClassVar, Iterator, Optional, Type, TypeVar, Union

from ..utils import lazy_import
from fileio.src import type_utils

tf = lazy_import('tensorflow')

_P = TypeVar('_P')


URI_PREFIXES = ('gs://', 's3://')
_URI_SCHEMES = frozenset(('gs', 's3'))

_URI_MAP_ROOT = {
        'gs://': '/gs/',
        's3://': '/s3/',
}


class _GPath(pathlib.PurePath, type_utils.ReadWritePath):
    """Pathlib like api around `tf.io.gfile`."""

    # `_PATH` is `posixpath` or `ntpath`.
    # Use explicit `join()` rather than `super().joinpath()` to avoid infinite
    # recursion.
    # Do not use `os.path`, so `PosixGPath('gs://abc')` works on windows.
    _PATH: ClassVar[types.ModuleType]

    def __new__(cls: Type[_P], *parts: type_utils.PathLike) -> _P:
        full_path = '/'.join(os.fspath(p) for p in parts)
        if not full_path.startswith(URI_PREFIXES):
            return super().__new__(cls, *parts)

        prefix = full_path[:5]
        new_prefix = _URI_MAP_ROOT[prefix]
        return super().__new__(cls, full_path.replace(prefix, new_prefix, 1))

    def _new(self: _P, *parts: type_utils.PathLike) -> _P:
        """Create a new `Path` child of same type."""
        return type(self)(*parts)

    # Could try to use `cached_property` when beam is compatible (currently
    # raise mutable input error).
    @property
    def _uri_scheme(self) -> Optional[str]:
        if (len(self.parts) >= 2 and self.parts[0] == '/' and
                self.parts[1] in _URI_SCHEMES):
            return self.parts[1]
        else:
            return None

    @property
    def _path_str(self) -> str:
        """Returns the `__fspath__` string representation."""
        uri_scheme = self._uri_scheme
        if uri_scheme:    # pylint: disable=using-constant-test
            return self._PATH.join(f'{uri_scheme}://', *self.parts[2:])
        else:
            return self._PATH.join(*self.parts) if self.parts else '.'

    def __fspath__(self) -> str:
        return self._path_str

    def __str__(self) -> str:    # pylint: disable=invalid-str-returned
        return self._path_str

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._path_str!r})'

    def exists(self) -> bool:
        """Returns True if self exists."""
        return tf.io.gfile.exists(self._path_str)

    def is_dir(self) -> bool:
        """Returns True if self is a directory."""
        return tf.io.gfile.isdir(self._path_str)

    def iterdir(self: _P) -> Iterator[_P]:
        """Iterates over the directory."""
        for f in tf.io.gfile.listdir(self._path_str):
            yield self._new(self, f)

    def expanduser(self: _P) -> _P:
        """Returns a new path with expanded `~` and `~user` constructs."""
        return self._new(self._PATH.expanduser(self._path_str))

    def resolve(self: _P, strict: bool = False) -> _P:
        """Returns the abolute path."""
        if self.is_cloud:
            return self._new(self.as_posix())
        return self._new(self._PATH.abspath(self._path_str))

    def glob(self: _P, pattern: str) -> Iterator[_P]:
        """Yielding all matching files (of any kind)."""
        for f in tf.io.gfile.glob(self._PATH.join(self._path_str, pattern)):
            yield self._new(f)

    def mkdir(
            self,
            mode: int = 0o777,
            parents: bool = False,
            exist_ok: bool = False,
    ) -> None:
        """Create a new directory at this given path."""
        if self.exists() and not exist_ok:
            raise FileExistsError(f'{self._path_str} already exists.')

        if parents:
            tf.io.gfile.makedirs(self._path_str)
        else:
            tf.io.gfile.mkdir(self._path_str)

    def rmdir(self) -> None:
        """Remove the empty directory."""
        if not self.is_dir():
            raise NotADirectoryError(f'{self._path_str} is not a directory.')
        if list(self.iterdir()):
            raise ValueError(f'Directory {self._path_str} is not empty')
        tf.io.gfile.rmtree(self._path_str)

    def rmtree(self) -> None:
        """Remove the directory."""
        tf.io.gfile.rmtree(self._path_str)

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove this file or symbolic link."""
        try:
            tf.io.gfile.remove(self._path_str)
        except tf.errors.NotFoundError as e:
            if not missing_ok:
                raise FileNotFoundError(str(e))

    def open(
            self,
            mode: str = 'r',
            encoding: Optional[str] = None,
            errors: Optional[str] = None,
            **kwargs: Any,
    ) -> typing.IO[Union[str, bytes]]:
        """Opens the file."""
        if errors:
            raise NotImplementedError
        if encoding and not encoding.lower().startswith(('utf8', 'utf-8')):
            raise ValueError(f'Only UTF-8 encoding supported. Not: {encoding}')
        gfile = tf.io.gfile.GFile(self._path_str, mode, **kwargs)
        gfile = typing.cast(typing.IO[Union[str, bytes]], gfile)
        return gfile

    def rename(self: _P, target: type_utils.PathLike) -> _P:
        """Rename file or directory to the given target."""
        # Note: Issue if WindowsPath and target is gs://. Rather than using `_new`,
        # `GPath.__new__` should dynamically return either `PosixGPath` or
        # `WindowsPath`, similarly to `pathlib.Path`.
        target = self._new(target)
        tf.io.gfile.rename(self._path_str, os.fspath(target))
        return target

    def replace(self: _P, target: type_utils.PathLike) -> _P:
        """Replace file or directory to the given target."""
        target = self._new(target)
        tf.io.gfile.rename(self._path_str, os.fspath(target), overwrite=True)
        return target

    def copy(self: _P, dst: type_utils.PathLike, overwrite: bool = False, skip_errors=False) -> _P:
        """Copies the File to the Dir/File."""
        # Could add a recursive=True mode
        dst = self._new(dst) if isinstance(dst, str) else dst
        if not dst.is_file(): dst = dst.parent.joinpath(self.name)
        try:
            tf.io.gfile.copy(self._path_str, os.fspath(dst), overwrite=overwrite)
            return dst
        except Exception as e:
            print(f'Error Copying {self._path_str} -> {dst.as_posix()}: {str(e)}')
            if skip_errors:
                return None
            raise ValueError
        
    def copydir(self: _P, dst: type_utils.PathLike, ignore=['.git'], overwrite: bool = False, dryrun: bool = False):
        """Copies the Current Top Level Parent Dir to the Dst Dir without recursion"""
        dst = self._new(dst)
        assert dst.is_dir(), 'Destination is not a valid directory'
        if not dryrun: dst.ensure_dir()
        copied_files = []
        fnames = self.listdir(ignore=ignore)
        curdir = self.absolute_parent
        for fname in fnames:
            dest_path = dst.joinpath(fname.relative_to(curdir))
            if not dryrun:
                fname.copy(dest_path, overwrite=overwrite, skip_errors=True)
            copied_files.append(dest_path)
        return copied_files


    def copydirs(self: _P, dst: type_utils.PathLike, mode: str = 'shallow', pattern='*', ignore=['.git'], overwrite: bool = False, levels: int = 2, dryrun: bool = False):
        """Copies the Current Parent Dir to the Dst Dir.
        modes = [shallow for top level recursive. recursive for all nested]
        levels = number of recursive levels
        dryrun = returns all files that would have been copied without copying
        """
        assert mode in {'shallow', 'recursive'}, 'Invalid Mode Option: [shallow, recursive]'
        dst = self._new(dst)
        assert dst.is_dir(), 'Destination is not a valid directory'
        levels = max(1, levels)
        dst.ensure_dir()
        curdir = self.absolute_parent
        copied_files = []
        if levels > 1 and mode == 'recursive' and '/' not in pattern:
            for _ in range(1, levels): pattern += '/*'
        if self.is_dir() and not pattern.startswith('/'): pattern = '*/' + pattern
        fiter = curdir.glob(pattern) if mode == 'shallow' else curdir.rglob(pattern)
        fnames = [f for f in fiter if not bool(set(f.parts).intersection(ignore))]
        print(len(fnames))
        for f in fnames:
            dest_path = dst.joinpath(f.relative_to(curdir))
            if not dryrun:
                if f.is_dir():
                    dest_path.ensure_dir()
                else:
                    f.copy(dest_path, overwrite=overwrite, skip_errors=True)
            copied_files.append(dest_path)
        return copied_files

    def listdir(self: _P, ignore=['.git'], skip_dirs=True, skip_files=False):
        fnames = [f for f in self.iterdir() if not bool(set(f.parts).intersection(ignore))]
        fnames = [f.resolve() for f in fnames]
        if skip_dirs:
            return [f for f in fnames if f.is_file()]
        if skip_files:
            return [f for f in fnames if f.is_dir()]
        return fnames

    def listdirs(self: _P, mode: str = 'shallow', pattern='*', ignore=['.git'], skip_dirs=True, skip_files=False, levels: int = 2):
        """Lists all files in current parent dir
        modes = [shallow for top level recursive. recursive for all nested]
        """
        assert mode in {'shallow', 'recursive'}, 'Invalid Mode Option: [shallow, recursive]'
        curdir = self.absolute_parent
        levels = max(1, levels)
        if levels > 1 and mode == 'recursive' and '/' not in pattern:
            for _ in range(1, levels): pattern += '/*'
        if self.is_dir() and not pattern.startswith('*/'): pattern = '*/' + pattern
        print(pattern)
        fiter = curdir.glob(pattern) if mode == 'shallow' else curdir.rglob(pattern)
        fnames = [f for f in fiter if not bool(set(f.parts).intersection(ignore))]
        print(len(fnames))
        if skip_dirs:
            return [f for f in fnames if f.is_file()]
        if skip_files:
            return [f for f in fnames if f.is_dir()]
        return [f for f in fnames]


    def ensure_dir(self: _P, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """Ensures the parent directory exists, creates if not"""
        return self.absolute_parent.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    @property
    def absolute_parent(self: _P) -> _P:
        uri_scheme = self._uri_scheme
        if uri_scheme:
            return self._new(self._PATH.join(f'{uri_scheme}://', '/'.join(self.parts[2:-1])))
        p = self.resolve()
        if p.is_dir: 
            return p
        return p.parent

    @property
    def is_cloud(self: _P) -> bool:
        return bool(self._uri_scheme)
    
    @property
    def is_gcs(self: _P) -> bool:
        return bool(self._uri_scheme == 'gs')
    
    @property
    def is_s3(self: _P) -> bool:
        return bool(self._uri_scheme == 's3')
    
    @property
    def file_ext(self: _P) -> bool:
        return self.suffix[1:]
    


class PosixGPath(_GPath, pathlib.PurePosixPath):
    """Pathlib like api around `tf.io.gfile`."""
    _PATH = posixpath


class WindowsGPath(_GPath, pathlib.PureWindowsPath):
    """Pathlib like api around `tf.io.gfile`."""
    _PATH = ntpath


PathIOLike = TypeVar("PathIOLike", str, Union[pathlib.Path, PosixGPath])
