from __future__ import annotations
import os
from .base import *


from .filesystem_base import AWS_CloudFileSystem
from .filesystem_pathlib import *


class FileS3PurePath(CloudFileSystemPurePath):
    _prefix: str = 's3'
    _provider: str = 'AmazonS3'
    _win_pathz: ClassVar = 'PureFileS3WindowsPath'
    _posix_pathz: ClassVar = 'PureFileS3PosixPath'


class PureFileS3PosixPath(PureCloudFileSystemPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PureFileS3WindowsPath(PureCloudFileSystemWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()

class FileS3Path(CloudFileSystemPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = 's3'
    _provider = 'AmazonS3'

    _win_pathz: ModuleType = 'FileS3WindowsPath'
    _posix_pathz: ModuleType = 'FileS3PosixPath'

    def _init(self, template: Optional['FileS3Path'] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is FileS3Path or issubclass(cls, FileS3Path): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self


class FileS3PosixPath(PosixPath, FileS3Path, PureFileS3PosixPath):
    __slots__ = ()


class FileS3WindowsPath(WindowsPath, FileS3Path, PureFileS3WindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("FileS3Path.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("FileS3Path.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        FileS3PurePath, FileS3Path, PureFileS3PosixPath, FileS3WindowsPath, FileS3PosixPath, PureFileS3WindowsPath
    ]
)

AWSFileSystem = AWS_CloudFileSystem


__all__ = (
    'FileS3PurePath',
    'FileS3Path',
    'PureFileS3PosixPath',
    'FileS3WindowsPath',
    'FileS3PosixPath',
    'PureFileS3WindowsPath',
    'AWSFileSystem'
)
