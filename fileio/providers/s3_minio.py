from __future__ import annotations

import os
from loguru import logger
from .base import *
from .filesystem_base import Minio_CloudFileSystem
from .filesystem_pathlib import *


class FileMinioPurePath(CloudFileSystemPurePath):
    _prefix: str = 'minio'
    _posix_prefix: str = 's3'
    _provider: str = 'Minio'
    _win_pathz: ClassVar = 'PureFileMinioWindowsPath'
    _posix_pathz: ClassVar = 'PureFileMinioPosixPath'


class PureFileMinioPosixPath(PureCloudFileSystemPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PureFileMinioWindowsPath(PureCloudFileSystemWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()

class FileMinioPath(CloudFileSystemPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = 'minio'
    _posix_prefix = 's3'
    _provider = 'Minio'

    _win_pathz: ModuleType = 'FileMinioWindowsPath'
    _posix_pathz: ModuleType = 'FileMinioPosixPath'

    def _init(self, template: Optional['FileMinioPath'] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is FileMinioPath or issubclass(cls, FileMinioPath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self

    @property
    def _cloudpath(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        #if self._prefix in self.parts[0]: return self._pathlike.join(*self.parts[1:])
        #return self._pathlike.join(*self.parts)
        if self._prefix in self.parts[0] or self._posix_prefix in self.parts[0]: return self._pathlike.join(*self.parts[1:])
        return self._pathlike.join(*self.parts)
    
    @property
    def _s3_cloudstr(self) -> str:
        """
        Reconstructs the proper cloud URI
        """
        if self._prefix not in self.parts[0] and self._posix_prefix not in self.parts[0]:
            return f'{self._posix_prefix}://' + '/'.join(self.parts)
        return f'{self._posix_prefix}://' + '/'.join(self.parts[1:])

    
    def glob(self, pattern: str = '*', as_path: bool = True) -> Iterable[Union[str, Type['CloudFileSystemPath']]]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        Warning: doesn't work as expected. Use Find Instead.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        #if self.is_cloud:
        glob_pattern = self._s3_cloudstr + ('/' if self.is_dir and not self._path.endswith('/') and not pattern.startswith('/') else '') +  pattern
        try: 
            matches =  self._accessor.glob(glob_pattern)
            if not matches: return matches
            if self.is_cloud: matches = [f'{self._prefix}://{m}' for m in matches]
            if as_path: matches = [type(self)(m) for m in matches]
            return matches
        except Exception as e: 
            logger.error(e)
            return self.find(pattern = pattern, as_string = not as_path)

    async def async_glob(self, pattern: str = '*', as_path: bool = True) -> AsyncIterable[Type['CloudFileSystemPath']]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        glob_pattern = self._s3_cloudstr + ('/' if self.is_dir and not self._path.endswith('/') and not pattern.startswith('/') else '') +  pattern
        try: 
            matches = await self._accessor.async_glob(glob_pattern)
            #logger.info(glob_pattern)
            if not matches: return matches
            if self.is_cloud: matches = [f'{self._prefix}://{m}' for m in matches]
            if as_path: matches = [type(self)(m) for m in matches]
            return matches
        except Exception as e: 
            logger.error(e)
            return await self.async_find(pattern = pattern, as_string = not as_path)
    
    def find(self, pattern: str = "*",  as_string: bool = False, maxdepth: int = None, withdirs: bool = None, detail: bool = False) -> Union[List[str], List[Type['CloudFileSystemPath']]]:
        """
        List all files below path. Like posix find command without conditions
        """
        matches = self._accessor.find(path = self._s3_cloudstr, maxdepth = maxdepth, withdirs = withdirs, detail = detail, prefix = pattern)
        if self.is_cloud:
            matches = [f'{self._prefix}://{m}' for m in matches]
        if not as_string:
            matches = [type(self)(m) for m in matches]
        return matches
    
    async def async_find(self, pattern: str = "*",  as_string: bool = False, maxdepth: int = None, withdirs: bool = None, detail: bool = False) -> Union[List[str], List[Type['CloudFileSystemPath']]]:
        """
        List all files below path. Like posix find command without conditions
        """
        matches = await self._accessor.async_find(path = self._s3_cloudstr, maxdepth = maxdepth, withdirs = withdirs, detail = detail, prefix = pattern)
        if self.is_cloud:
            matches = [f'{self._prefix}://{m}' for m in matches]
        if not as_string:
            matches = [type(self)(m) for m in matches]
        return matches


class FileMinioPosixPath(PosixPath, FileMinioPath, PureFileMinioPosixPath):
    __slots__ = ()


class FileMinioWindowsPath(WindowsPath, FileMinioPath, PureFileMinioWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("FileMinioPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("FileMinioPath.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        FileMinioPurePath, FileMinioPath, PureFileMinioPosixPath, FileMinioWindowsPath, FileMinioPosixPath, PureFileMinioWindowsPath
    ]
)

MinioFileSystem = Minio_CloudFileSystem


__all__ = (
    'FileMinioPurePath',
    'FileMinioPath',
    'PureFileMinioPosixPath',
    'FileMinioWindowsPath',
    'FileMinioPosixPath',
    'PureFileMinioWindowsPath',
    'MinioFileSystem'
)
