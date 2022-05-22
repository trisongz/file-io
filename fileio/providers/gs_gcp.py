from __future__ import annotations

import os
from .base import *
from .filesystem_base import GCP_CloudFileSystem
from .filesystem_pathlib import *


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
