from __future__ import annotations

import os
from fileio.providers.base import *
from fileio.providers.filesys import GCP_CloudFileSystem
from fileio.providers.filesys_cloud import *
from fileio.utils import logger

class FileGSPurePath(CloudFileSystemPurePath):
    _prefix: str = 'gs'
    _provider: str = 'GoogleCloudStorage'
    _win_pathz: ClassVar = 'PureFileGSWindowsPath'
    _posix_pathz: ClassVar = 'PureFileGSPosixPath'


class PureFileGSPosixPath(PureCloudFileSystemPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()
    _prefix = 'gs'
    _provider = 'GoogleCloudStorage'


class PureFileGSWindowsPath(PureCloudFileSystemWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()
    _prefix = 'gs'
    _provider = 'GoogleCloudStorage'

class FileGSPath(CloudFileSystemPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = 'gs'
    _provider = 'GoogleCloudStorage'

    _win_pathz: ModuleType = 'FileGSWindowsPath'
    _posix_pathz: ModuleType = 'FileGSPosixPath'

    def _init(self, template: Optional['FileGSPath'] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None
        self._has_tffs = self._accessor.tffs is not None

    def __new__(cls, *parts, **kwargs):
        if cls is FileGSPath or issubclass(cls, FileGSPath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self
    
    def gfile(self, mode: str = 'r', **kwargs: Any):
        return self._accessor.tffs.GFile(self.string, mode = mode, **kwargs) if self._has_tffs else self.open(mode = mode, **kwargs)

    def glob(self, pattern: str = '*', as_path: bool = True) -> Iterable[Union[str, Type['CloudFileSystemPath']]]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        Warning: doesn't work as expected. Use Find Instead.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        #if self.is_cloud:
        if self._has_tffs:
            glob_pattern = self.joinpath(pattern).string
            # logger.info(f'Using TFFS for globbing: {glob_pattern}')
            matches = self._accessor.tffs.glob(pattern = glob_pattern)
            if not matches: return matches
            if as_path: matches = [type(self)(m) for m in matches]
            return matches
        return super().glob(pattern = pattern, as_path = as_path)

    async def async_glob(self, pattern: str = '*', as_path: bool = True) -> AsyncIterable[Type['CloudFileSystemPath']]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        if self._has_tffs:
            glob_pattern = self.joinpath(pattern).string
            # logger.info(f'Using TFFS for globbing: {glob_pattern}')
            matches = await self._accessor.tffs.async_glob(pattern = glob_pattern)
            if not matches: return matches
            if as_path: matches = [type(self)(m) for m in matches]
            return matches
        
        return await super().async_glob(pattern = pattern, as_path = as_path)

    def read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS) -> str:
        """Read the contents of this file as a string.
        """
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'r') as file:
                return file.read()
        return super().read_text(encoding=encoding, errors=errors)

    async def async_read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS) -> str:
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'r') as file:
                return await file.async_read()
        return await super().async_read_text(encoding=encoding, errors=errors)

    def write_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'w') as file:
                return file.write(data)
        return super().write_text(data, encoding=encoding, errors=errors, newline=newline)

    async def async_write_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'w') as file:
                return await file.async_write(data)
        return await super().async_write_text(data, encoding=encoding, errors=errors, newline=newline)

    def read_bytes(self) -> bytes:
        """Read the contents of this file as bytes.
        """
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'rb') as file:
                return file.read()
        return super().read_bytes()

    async def async_read_bytes(self) -> bytes:
        """Read the contents of this file as bytes."""
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'rb') as file:
                return await file.async_read()
        return await super().async_read_bytes()

    def write_bytes(self, data: bytes) -> int:
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'wb') as file:
                return file.write(data)
        return super().write_bytes(data)

    async def async_write_bytes(self, data: bytes) -> int:
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        if self._has_tffs:
            with self._accessor.tffs.GFile(self.string, 'wb') as file:
                return await file.async_write(data)
        return await super().async_write_bytes(data)



class FileGSPosixPath(PosixPath, FileGSPath, PureFileGSPosixPath):
    __slots__ = ()


class FileGSWindowsPath(WindowsPath, FileGSPath, PureFileGSWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("FileGSPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("FileGSPath.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        FileGSPurePath, FileGSPath, PureFileGSPosixPath, FileGSWindowsPath, FileGSPosixPath, PureFileGSWindowsPath
    ]
)

GCPFileSystem = GCP_CloudFileSystem


__all__ = (
    'FileGSPurePath',
    'FileGSPath',
    'PureFileGSPosixPath',
    'FileGSWindowsPath',
    'FileGSPosixPath',
    'PureFileGSWindowsPath',
    'GCPFileSystem'
)
