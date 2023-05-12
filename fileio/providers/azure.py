from __future__ import annotations

"""
Azure Provider for FileIO
"""

import os

from fileio.lib.posix.base import *
from fileio.lib.posix.filesys import Azure_CloudFileSystem
from fileio.lib.posix.cloud import *

class FileAZPurePath(CloudFileSystemPurePath):
    _prefix: str = 'az'
    _provider: str = 'Azure'
    _win_pathz: ClassVar = 'PureFileAZWindowsPath'
    _posix_pathz: ClassVar = 'PureFileAZPosixPath'


class PureFileAZPosixPath(PureCloudFileSystemPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PureFileAZWindowsPath(PureCloudFileSystemWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()

class FileAZPath(CloudFileSystemPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = 'az'
    _provider = 'Azure'

    _win_pathz: ModuleType = 'FileAZWindowsPath'
    _posix_pathz: ModuleType = 'FileAZPosixPath'

    def _init(self, template: Optional['FileAZPath'] = None):
        self._accessor: AccessorLike = FileSysManager.get_accessor(self._prefix)
        # self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is FileAZPath or issubclass(cls, FileAZPath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self
    

class FileAZPosixPath(PosixPath, FileAZPath, PureFileAZPosixPath):
    __slots__ = ()


class FileAZWindowsPath(WindowsPath, FileAZPath, PureFileAZWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("FileAZPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("FileAZPath.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        FileAZPurePath, FileAZPath, PureFileAZPosixPath, FileAZWindowsPath, FileAZPosixPath, PureFileAZWindowsPath
    ]
)

AZFileSystem = Azure_CloudFileSystem


__all__ = (
    'FileAZPurePath',
    'FileAZPath',
    'PureFileAZPosixPath',
    'FileAZWindowsPath',
    'FileAZPosixPath',
    'PureFileAZWindowsPath',
    'AZFileSystem'
)
