from __future__ import annotations

import os
import contextlib
from fileio.lib.posix.base import *
from fileio.lib.posix.filesys import R2_CloudFileSystem, R2_Accessor
from fileio.lib.posix.cloud import *
from fileio.utils import logger
from typing import Mapping, TYPE_CHECKING

if TYPE_CHECKING:
    from fileio.lib.types import FileLike

class FileR2PurePath(CloudFileSystemPurePath):
    _prefix: str = 'r2'
    _provider: str = 'CloudFlare'
    _win_pathz: ClassVar = 'PureFileR2WindowsPath'
    _posix_pathz: ClassVar = 'PureFileR2PosixPath'


class PureFileR2PosixPath(PureCloudFileSystemPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PureFileR2WindowsPath(PureCloudFileSystemWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()


class FileR2Path(CloudFileSystemPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: R2_Accessor = None
    _pathlike = posixpath
    _prefix = 'r2'
    _provider = 'CloudFlare'

    _win_pathz: ModuleType = 'FileR2WindowsPath'
    _posix_pathz: ModuleType = 'FileR2PosixPath'

    def _init(self, template: Optional['FileR2Path'] = None):
        self._accessor: R2_Accessor = FileSysManager.get_accessor(self._prefix)
        # self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is FileR2Path or issubclass(cls, FileR2Path): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self
    

    # Implement some stuff that boto is faster in
    def upload_file(self, dest: 'FileLike', filename: Optional[str] = None, overwrite: bool = True, **kwargs):
        """
        Upload a file to R2

        Utilize boto3
        """
        if not overwrite and dest.exists(): raise FileExistsError(f"{dest} already exists and overwrite is False")
        filename = filename or self.name
        self._accessor.boto.upload_file(
            Bucket = self._bucket,
            Key = self.get_path_key(filename),
            Filename = dest.as_posix(),
        )
        return self.parent.joinpath(filename)
    
    def download_file(
        self, 
        output_file: Optional['FileLike'] = None,
        output_dir: Optional['FileLike'] = None,
        filename: Optional[str] = None,
        overwrite: bool = True,
        callbacks: Optional[List[Any]] = None,
        **kwargs
        ):
        """
        Downloads a file from R2 to a path
        """
        assert output_file or output_dir, "Must provide either output_file or output_dir"
        output_file = output_file or output_dir.joinpath(filename or self.name)
        assert overwrite or not output_file.exists(), f"{output_file} already exists and overwrite is False"
        # s3t = self._accessor.s3t()
        self._accessor.s3t.download(
            self._bucket,
            self.get_path_key(self.name),
            output_file.as_posix(),
            subscribers = callbacks
        )
        # self._accessor.s3t.shutdown()
        return output_file

    async def async_upload_file(
        self, 
        source: 'FileLike', 
        filename: Optional[str] = None,  
        overwrite: bool = True, 
        **kwargs
    ):
        """
        Upload a file to R2

        Utilize boto3
        """
        if not overwrite and await source.async_exists(): raise FileExistsError(f"{source} already exists and overwrite is False")
        filename = filename or self.name
        # s3t = self._accessor.s3t()
        self._accessor.s3t.upload(
            source.as_posix(),
            self._bucket,
            self.get_path_key(filename)
        )
        # self._accessor.s3t.shutdown()
        #await to_thread(
        #    self._accessor.boto.upload_file, Bucket = self._bucket, Key = self.get_path_key(filename), Filename = dest.as_posix()
        #)
        return self.parent.joinpath(filename)

    def batch_upload_files(
        self, 
        files: Optional[List['FileLike']] = None,
        glob_path: Optional[str] = None,
        overwrite: bool = False,
        skip_existing: bool = True,
        callbacks: Optional[List[Any]] = None,
        **kwargs
    ):
        """
        Handles batch uploading of files

        https://stackoverflow.com/questions/56639630/how-can-i-increase-my-aws-s3-upload-speed-when-using-boto3
        """
        assert files or glob_path, "Must provide either files or glob_path"
        if glob_path: files = list(self.glob(glob_path))
        results = []
        # s3t = self._accessor.s3t()
        for file in files:
            if not overwrite and skip_existing and file.exists(): continue
            self._accessor.s3t.upload(
                file.as_posix(),
                self._bucket,
                self.get_path_key(file.name),
                subscribers = callbacks
            )
            results.append(self.parent.joinpath(file.name))
        # self._accessor.s3t.shutdown()
        return results
    
    def batch_download_files(
        self,
        glob_path: str,
        output_dir: 'FileLike',
        overwrite: bool = False,
        skip_existing: bool = True,
        callbacks: Optional[List[Any]] = None,
        **kwargs
    ):
        """
        Handles batch downloading of files
        """
        files = list(self.glob(glob_path))
        results = []
        # s3t = self._accessor.s3t()
        for file in files:
            if not overwrite and skip_existing and file.exists(): continue
            output_file = output_dir.joinpath(file.name)
            self._accessor.s3t.download(
                self._bucket,
                self.get_path_key(file.name),
                output_file.as_posix(),
                subscribers = callbacks
            )
            results.append(output_file)
        self._accessor.s3t.shutdown()
        return results


    def unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: self.filesys.rm_file(self._cloudpath)
        except FileNotFoundError:
            if not missing_ok: raise

    async def async_unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: await self.afilesys._rm_file(self._cloudpath)
        except FileNotFoundError:
            if not missing_ok: raise

    def rm_file(self, missing_ok: bool = True):
        """
        Remove this file.
        If the path is a directory, use rmdir() instead.
        """
        try:
            self.filesys.rm_file(self._cloudpath)
            return True
        except Exception as e:
            if missing_ok: return False
            raise e from e

    async def async_rm_file(self, missing_ok: bool = True):
        """
        Remove this file.
        If the path is a directory, use rmdir() instead.
        """
        try:
            await self.afilesys._rm_file(self._cloudpath)
            return True
        except Exception as e:
            if missing_ok: return False
            raise e from e


class FileR2PosixPath(PosixPath, FileR2Path, PureFileR2PosixPath):
    __slots__ = ()


class FileR2WindowsPath(WindowsPath, FileR2Path, PureFileR2WindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("FileR2Path.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("FileR2Path.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        FileR2PurePath, FileR2Path, PureFileR2PosixPath, FileR2WindowsPath, FileR2PosixPath, PureFileR2WindowsPath
    ]
)

R2FileSystem = R2_CloudFileSystem


__all__ = (
    'FileR2PurePath',
    'FileR2Path',
    'PureFileR2PosixPath',
    'FileR2WindowsPath',
    'FileR2PosixPath',
    'PureFileR2WindowsPath',
    'R2FileSystem'
)
