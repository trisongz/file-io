from __future__ import annotations

import os
import hashlib
import datetime
import functools
import asyncio
from typing import ClassVar
from fsspec.callbacks import Callback
from pydantic.types import ByteSize
from fileio.lib.aiopath.wrap import to_thread
from fileio.lib.flavours import _pathz_windows_flavour, _pathz_posix_flavour

from fileio.lib.posix.base import *
from fileio.lib.posix.filesys import AccessorLike, CloudFileSystemLike, FileSysManager
import fileio.lib.exceptions as exceptions
from fileio.utils import logger

if TYPE_CHECKING:
    from fileio.lib.types import PathLike, FileLike
    from fsspec.asyn import AsyncFileSystem
    # from fileio.core.generic import PathLike


class CloudFileSystemPurePath(PurePath):
    _prefix: str = None
    _provider: str = None
    _win_pathz: ClassVar = 'PureCloudFileSystemWindowsPath'
    _posix_pathz: ClassVar = 'PureCloudFileSystemPosixPath'

    def _init(self, template: Optional[PurePath] = None):
        self._accessor: AccessorLike = FileSysManager.get_accessor(self._prefix)
        # self._accessor: AccessorLike = get_accessor(self._prefix)

    def __new__(cls, *args):
        if cls is CloudFileSystemPurePath or issubclass(cls, CloudFileSystemPurePath):
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        return cls._from_parts(args)

    def _new(self, *parts):
        """Create a new `Path` child of same type."""
        return type(self)(*parts)


class PureCloudFileSystemPosixPath(CloudFileSystemPurePath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PureCloudFileSystemWindowsPath(CloudFileSystemPurePath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()


class CloudFileSystemPath(Path, CloudFileSystemPurePath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = None
    _provider = None
    _win_pathz: ClassVar = 'CloudFileSystemWindowsPath'
    _posix_pathz: ClassVar = 'CloudFileSystemPosixPath'

    def _init(self, template: Optional['CloudFileSystemPath'] = None):
        self._accessor: AccessorLike = FileSysManager.get_accessor(self._prefix)
        # self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is CloudFileSystemPath or issubclass(cls, CloudFileSystemPath):
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.string}")'

    def __str__(self):
        return self.string

    @property
    def _path(self) -> str:
        """
        Returns the path as a string
        """
        return self._cloudstr if self.is_cloud else str(self)
    
    @property
    def filesys(self) -> Optional['AsyncFileSystem']:
        """
        Returns the filesystem object
        """
        return getattr(self._accessor, 'filesys', None)

    @property
    def afilesys(self) -> Optional['AsyncFileSystem']:
        """
        The async filesystem object associated with this path.
        """
        return getattr(self._accessor, 'async_filesys', None)

    @property
    def _cloudpath(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        if self._prefix in self.parts[0]: return self._pathlike.join(*self.parts[1:])
        return self._pathlike.join(*self.parts)

    @property
    def _bucket(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        return self.parts[1] if self._prefix in self.parts[0] \
            else self.parts[0]

    @property
    def _bucketstr(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        return f'{self._prefix}://{self._bucket}'

    @property
    def _pathkeys(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        if self._bucket in self.parts[0]: return self._pathlike.join(*self.parts[1:])
        if self._bucket in self.parts[1]: return self._pathlike.join(*self.parts[2:])
        return self._pathlike.join(*self.parts)

    def get_path_key(self, filename: Optional[str] = None) -> str:
        """
        Used to return relative/path/to/file.ext
        """
        filename = filename or self.name
        parts = None
        if self._bucket in self.parts[0]: parts = self.parts[1:-1]
        elif self._bucket in self.parts[1]: parts = self.parts[2:-1]
        else: parts = self.parts[:-1]
        return self._pathlike.join(*parts, filename)

    @property
    def _cloudstr(self) -> str:
        """
        Reconstructs the proper cloud URI
        """
        if self._prefix not in self.parts[0]:
            return f'{self._prefix}://' + '/'.join(self.parts)
        return f'{self._prefix}://' + '/'.join(self.parts[1:])

    @property
    def posix_(self):
        """Return the string representation of the path with forward (/)
        slashes."""
        f = self._flavour
        return str(self).replace(f.sep, '/')

    @property
    def string(self) -> str:
        return self._cloudstr if self.is_cloud else self.posix_

    @property
    def filename_(self) -> str:
        """
        Returns the filename if is file, else ''
        """
        return self.parts[-1] if self.is_file() else ''

    @property
    def ext_(self) -> str:
        """
        Returns the extension for a file
        """
        return self.suffix

    @property
    def extension(self) -> str:
        """
        Returns the extension for a file
        """
        return self.suffix

    @property
    def stat_(self) -> stat_result:
        """
        Returns the stat results for path
        """
        return self.stat()

    @property
    def hash_(self) -> str:
        """
        Hash of file properties, to tell if it has changed
        """
        return self._accessor.ukey(self._cloudpath)

    @property
    def info_(self):
        """
        Return info of path
        """
        return self._accessor.info(path=self._cloudpath)

    @property
    def metadata_(self):
        """
        Return metadata of path
        """
        return self._accessor.metadata(self._cloudpath)

    @property
    def path_info_(self):
        """
        Return info of path
        """
        return self._accessor.info(path=self._cloudpath)


    @property
    def size_(self) -> Optional[Union[float, int]]:
        """
        Size in bytes of file
        """
        return self._accessor.size(self._cloudpath) if self.is_file_ else None

    @property
    def modified_(self) -> 'datetime.datetime':
        """
        Return the last modified timestamp of file at path as a datetime
        """
        r = self.stat_ #self._accessor.modified(self._cloudpath)
        ts = r.get('updated', '')
        return datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ') if ts else None
        #return r.get('updated')

    @property
    def checksum(self):
        return self._accessor.checksum(path=self._cloudpath)

    @property
    def last_modified(self) -> 'datetime.datetime':
        ts = self.info_.get('LastModified')
        return ts or self.modified_

    @property
    def etag(self):
        """
        returns the file etag
        """
        rez = self.path_info_.get('ETag')
        if rez: rez = rez.replace('"', '').strip()
        return rez

    @property
    def file_size(self):
        """
        returns the total file size in bytes
        """
        return self.path_info_.get('Size')

    @property
    def content_type(self):
        """
        returns the ContentType attribute of the file
        """
        return self.path_info_.get('ContentType')

    @property
    def object_type(self):
        """
        returns the Type attribute of the file
        """
        return self.path_info_.get('type')

    @property
    def is_cloud(self) -> bool:
        return self._prefix in self.parts[0] or self._prefix in self.parts[1] if self._prefix else False

    @property
    def is_git(self) -> bool:
        return self.is_cloud and \
            f'{self._prefix}://' in GIT_PREFIXES

    @property
    def is_pathz(self) -> bool:
        return True

    @property
    def exists_(self) -> bool:
        """
        Returns True if path exists
        """
        return self.exists()

    @property
    def is_file_(self) -> bool:
        return self.is_file()

    @property
    def is_dir_(self) -> bool:
        return self.is_dir()

    @property
    def home_(self) -> Type['CloudFileSystemPath']:
        return self.home()

    @property
    async def async_exists_(self) -> bool:
        return await self.async_exists()

    @property
    async def async_is_file_(self) -> bool:
        return await self.async_is_file()

    @property
    async def async_is_dir_(self) -> bool:
        return await self.async_is_dir()

    @property
    async def async_home_(self) -> Type['CloudFileSystemPath']:
        return await self.async_home()

    @property
    async def async_stat_(self) -> stat_result:
        """
        Returns the stat results for path
        """
        return await self.async_stat()

    @property
    async def async_hash_(self) -> str:
        """
        Hash of file properties, to tell if it has changed
        """
        return await self._accessor.async_ukey(self._cloudpath)

    @property
    async def async_size_(self) -> Optional[Union[float, int]]:
        """
        Size in bytes of file
        """
        if await self.async_is_file_: return await self._accessor.async_size(self._cloudpath)
        return None

    @property
    async def async_metadata_(self):
        """
        Return metadata of path
        """
        return await self._accessor.async_metadata(self._cloudpath)

    @property
    async def async_modified_(self) -> 'datetime.datetime':
        """
        Return the last modified timestamp of file at path as a datetime
        """
        if self._prefix == 'gs':
            r = await self.async_stat_
            ts = r.get('updated', '')
            return datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ') if ts else ts
        return await self._accessor.async_modified(self._cloudpath)

    @property
    async def async_path_info_(self):
        """
        Return info of path
        """
        return await self.async_info()

    def open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)


    def async_open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        compression = infer doesn't work all that well.
        """
        #self._fileio = self._accessor.open(self._cloudpath, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, buffering=buffering, **kwargs)
        #print(type(self._fileio))
        #return get_cloud_file(self._fileio)
        # Test v2
        # return get_cloudfs_file(
        #     self._accessor,
        #     self._cloudpath, 
        #     mode=mode, 
        #     encoding=encoding, 
        #     errors=errors, 
        #     block_size=block_size, 
        #     compression=compression, 
        #     newline=newline, 
        #     buffering=buffering, 
        #     **kwargs
        # )
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, buffering=buffering, **kwargs))


    def reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs)

    def async_reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs))

    def appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs)

    def async_appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs))

    def writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        #self.touch()
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs)

    def async_writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        #self.touch()
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs))

    def read(self, mode: FileMode = 'rb', size: Optional[int] = -1, offset: Optional[int] = 0, **kwargs) -> Union[str, bytes]:
        """
        Read and return the file's contents.
        """
        with self.open(mode=mode, **kwargs) as file:
            return file.read(size, offset)
        
    async def async_read(self, mode: FileMode = 'rb', size: Optional[int] = -1, offset: Optional[int] = 0, **kwargs):
        """
        Read and return the file's contents.
        """
        async with self.async_open(mode=mode, **kwargs) as file:
            return await file.read(size, offset)

    def read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS, **kwargs) -> str:
        """
        Read and return the file's contents.
        """
        if hasattr(self.filesys, 'read_text'):
            return self.filesys.read_text(self._cloudpath, encoding=encoding, errors=errors, **kwargs)
        with self.open('r', encoding=encoding, errors=errors) as file:
            return file.read()

    async def async_read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS, **kwargs) -> str:
        """
        Read and return the file's contents.
        """
        async with self.async_open('r', encoding=encoding, errors=errors, **kwargs) as file:
            return await file.read()

    def read_bytes(self, start: Optional[Any] = None, end: Optional[Any] = None, **kwargs) -> bytes:
        """
        Read and return the file's contents.
        """
        if hasattr(self.filesys, 'read_bytes'):
            return self.filesys.read_bytes(self._cloudpath, start=start, end=end)
        return self._accessor.cat_file(self._cloudpath, start = start, end = end, **kwargs)

    async def async_read_bytes(self, start: Optional[Any] = None, end: Optional[Any] = None, **kwargs) -> bytes:
        """
        Read and return the file's contents.
        """
        # async with self.async_open('rb') as file:
        #     return await file.read()
        
        return await self._accessor.async_cat_file(self._cloudpath, start = start, end = end, **kwargs)
        

    def write_bytes(self, data: bytes) -> int:
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode='wb') as f:
            return f.write(data)

    async def async_write_bytes(self, data: bytes) -> int:
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        async with self.async_open(mode='wb') as f:
            return await f.write(data)

    def append_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        with self.open(mode='a', encoding=encoding, errors=errors, newline=newline) as f:
            n = f.write(data)
            n += f.write(newline)
            return n

    async def async_append_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        async with self.async_open(mode='a', encoding=encoding, errors=errors, newline=newline) as f:
            n = await f.write(data)
            n += await f.write(newline)
            return n

    def write_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        
        # Check if the filesys has `write_text` method
        if hasattr(self.filesys, 'write_text'):
            return self.filesys.write_text(self._cloudpath, data, encoding=encoding, errors=errors, newline=newline)
        with self.open(mode='w', encoding=encoding, errors=errors, newline=newline) as f:
            return f.write(data)

    async def async_write_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        async with self.async_open(mode='w', encoding=encoding, errors=errors, newline=newline) as f:
            return await f.write(data)


    def touch(self, truncate: bool = True, data = None, exist_ok: bool = True, **kwargs):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: self._accessor.stat(self._cloudpath)
            # Avoid exception chaining
            except OSError: pass
            else: return
        try:
            self._accessor.touch(self._cloudpath, truncate = truncate, data = data, **kwargs)
        except Exception as e:
            with self.open('wb') as f:
                f.write(b'')
                f.flush()


    async def async_touch(self, truncate: bool = True, data = None, exist_ok: bool = True, **kwargs):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: await self._accessor.async_stat(self._cloudpath)
            # Avoid exception chaining
            except OSError: pass
            else: return
        await self._accessor.async_touch(self._cloudpath, truncate = truncate, data = data, **kwargs)

    def mkdir(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: self._accessor.mkdir(self._cloudpath, parents = parents, exist_ok = exist_ok)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            self.parent.mkdir(parents=True, exist_ok=True)
            self.mkdir(mode, parents=False, exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not self.is_dir(): raise

    async def async_mkdir(self, parents: bool = True, exist_ok: bool = True, **kwargs):
        """
        Create a new directory at this given path.
        """
        try: await self._accessor.async_mkdir(self._cloudpath, create_parents = parents, exist_ok = exist_ok, **kwargs)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            await self.parent.async_mkdir(parents=True, exist_ok=True, **kwargs)
            await self.async_mkdir(parents=False, exist_ok=exist_ok, **kwargs)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not await self.async_is_dir(): raise

    async def async_makedirs(self, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: await self._accessor.async_makedirs(self._cloudpath, exist_ok = exist_ok)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            await self.parent.async_makedirs(exist_ok=True)
            await self.async_makedirs(exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not await self.async_is_dir(): raise

    async def chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        raise NotImplementedError

    async def async_chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        raise NotImplementedError

    def lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        raise NotImplementedError

    async def async_lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        raise NotImplementedError

    def unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: self._accessor.rm_file(self._cloudpath)
        except FileNotFoundError:
            if not missing_ok: raise

    async def async_unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: await self._accessor.async_rm_file(self._cloudpath)
        except FileNotFoundError:
            if not missing_ok: raise

    def rm(self, recursive: bool = False, maxdepth: int = None, missing_ok: bool = False):
        """
        Remove this file.
        If the path is a directory, use rmdir() instead.
        """
        try: self._accessor.rm(self._cloudpath, recursive = recursive, maxdepth = maxdepth)
        except Exception as e:
            if not missing_ok: raise e


    async def async_rm(self, recursive: bool = False, maxdepth: int = None, missing_ok: bool = False):
        """
        Remove this file.
        If the path is a directory, use rmdir() instead.
        """
        try: await self._accessor.async_rm(self._cloudpath, recursive = recursive, maxdepth = maxdepth)
        except Exception as e:
            if not missing_ok: raise e

    def rm_file(self, missing_ok: bool = True):
        """
        Remove this file.
        If the path is a directory, use rmdir() instead.
        """
        try:
            self._accessor.rm_file(self._cloudpath)
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
            await self._accessor.async_rm_file(self._cloudpath)
            return True
        except Exception as e:
            if missing_ok: return False
            raise e from e


    # async def async_unlink(self, missing_ok: bool = False):
    #     """
    #     Remove this file or link.
    #     If the path is a directory, use rmdir() instead.
    #     """
    #     try: await self._accessor.async_unlink(self._cloudpath, missing_ok = missing_ok)
    #     except FileNotFoundError:
    #         if not missing_ok: raise

    def rmdir(self, force: bool = False, recursive: bool = True, skip_errors: bool = True):
        """
        Remove this directory.  The directory must be empty.
        """
        try:
            return self._accessor.rmdir(self._cloudpath)
        except Exception as e:
            if force: return self._accessor.rmdir(self._cloudpath, recursive = recursive)
            if skip_errors: return
            raise e


    async def async_rmdir(self, force: bool = False, recursive: bool = True, skip_errors: bool = True):
        """
        Remove this directory.  The directory must be empty.
        """
        try:
            return await self._accessor.async_rmdir(self._cloudpath)
        except Exception as e:
            if force: return await self._accessor.async_rmdir(self._cloudpath, recursive = recursive)
            if skip_errors: return
            raise e

    def link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        raise NotImplementedError

    async def async_link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        raise NotImplementedError

    def rename(self, target: Union[str, Type['CloudFileSystemPath']]) -> Type['CloudFileSystemPath']:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.rename(self._cloudpath, target)
        return type(self)(target)

    async def async_rename(self, target: Union[str, Type['CloudFileSystemPath']]) -> Type['CloudFileSystemPath']:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_rename(self._cloudpath, target)
        return type(self)(target)

    def replace(self, target: str) -> Type['CloudFileSystemPath']:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.replace(self._cloudpath, target)
        return type(self)(target)

    async def async_replace(self, target: str) -> Type['CloudFileSystemPath']:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_replace(self._cloudpath, target)
        return type(self)(target)

    def symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        raise NotImplementedError

    async def async_symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        raise NotImplementedError

    def exists(self) -> bool:
        """
        Whether this path exists.
        """
        return self._accessor.exists(self._cloudpath)


    async def async_exists(self) -> bool:
        """
        Whether this path exists.
        """
        return await self._accessor.async_exists(self._cloudpath)

    @classmethod
    def cwd(cls: type) -> str:
        """Return a new path pointing to the current working directory
        (as returned by os.getcwd()).
        """
        cwd: str = os.getcwd()
        return cls(cwd)

    @classmethod
    def home(cls: type) -> Type['CloudFileSystemPath']:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        homedir: str = cls()._flavour.gethomedir(None)
        return cls(homedir)

    @classmethod
    async def async_home(cls: type) -> Type['CloudFileSystemPath']:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        coro = cls()._flavour.async_gethomedir(None)
        homedir: str = await coro
        return cls(homedir)

    def samefile(self, other_path: Union[Type['CloudFileSystemPath'], Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = Type['CloudFileSystemPath'](other_path)
        if isinstance(other_path, Type['CloudFileSystemPath']):
            try: other_st = other_path.stat()
            except AttributeError: other_st = self._accessor.stat(other_path)

        else:
            try: other_st = other_path.stat()
            except AttributeError: other_st = other_path._accessor.stat(other_path)
        return os.path.samestat(self.stat(), other_st)

    async def async_samefile(self, other_path: Union[Type['CloudFileSystemPath'], Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = Type['CloudFileSystemPath'](other_path)
        if isinstance(other_path, Type['CloudFileSystemPath']):
            try: other_st = await other_path.async_stat()
            except AttributeError: other_st = await self._accessor.async_stat(other_path)

        else:
            try: other_st = await to_thread(other_path.stat)
            except AttributeError: other_st = await to_thread(other_path._accessor.stat, other_path)

        return os.path.samestat(await self.async_stat(),other_st)

    def listdir(self) -> List[Type['CloudFileSystemPath']]:
        """Return a list of the entries in the directory (as returned by
        os.listdir()).
        """
        # return self._accessor.listdir(self._cloudpath)
        return [self._make_child_relpath(name) for name in self._accessor.listdir(self._cloudpath)]

    async def async_listdir(self) -> List[Type['CloudFileSystemPath']]:
        """Return a list of the entries in the directory (as returned by
        os.listdir()).
        """
        return [self._make_child_relpath(name) for name in await self._accessor.async_listdir(self._cloudpath)]
        # return [self._make_child_relpath(name) for name in await self._accessor.async_listdir(self._cloudstr)]


    def iterdir(self) -> Iterable[Type['CloudFileSystemPath']]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        for name in self._accessor.listdir(self._cloudpath):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)

    async def async_iterdir(self) -> AsyncIterable[Type['CloudFileSystemPath']]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        # for name in await self._accessor.async_listdir(self):
        async for name in self._accessor.async_listdir(self._cloudpath):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)
    

    def walk(self) -> Iterable[Tuple['CloudFileSystemPath', List['CloudFileSystemPath'], List['CloudFileSystemPath']]]:
        """Iterate over this subtree and yield a 3-tuple (dirpath, dirnames,
        filenames) for each directory in the subtree rooted at path
        (including path itself, if it is a directory).
        """
        top = self._make_child_relpath('.')
        dirs, nondirs = [], []
        for name in self._accessor.listdir(self._cloudpath):
            if name in {'.', '..'}: continue
            (dirs if self._accessor.is_dir(self._make_child_relpath(name)) else nondirs).append(self._make_child_relpath(name))
        yield top, dirs, nondirs
        for name in dirs:
            new_path = self._make_child_relpath(name)
            yield from new_path.walk()
        
    async def async_walk(self) -> AsyncIterable[Tuple['CloudFileSystemPath', List['CloudFileSystemPath'], List['CloudFileSystemPath']]]:
        """Iterate over this subtree and yield a 3-tuple (dirpath, dirnames,
        filenames) for each directory in the subtree rooted at path
        (including path itself, if it is a directory).
        """
        top = self._make_child_relpath('.')
        dirs, nondirs = [], []
        for name in await self._accessor.async_listdir(self._cloudpath):
            if name in {'.', '..'}: continue
            (dirs if await self._accessor.async_is_dir(self._make_child_relpath(name)) else nondirs).append(self._make_child_relpath(name))
        yield top, dirs, nondirs
        for name in dirs:
            new_path = self._make_child_relpath(name)
            async for path in new_path.async_walk():
                yield path

    def glob(self, pattern: str = '*', as_path: bool = True) -> Iterable[Union[str, Type['CloudFileSystemPath']]]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        Warning: doesn't work as expected. Use Find Instead.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        #if self.is_cloud:
        glob_pattern = self._cloudpath + ('/' if self.is_dir() and not self._cloudpath.endswith('/') and not pattern.startswith('/') else '') +  pattern
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
        glob_pattern = self._cloudpath + ('/' if self.is_dir() and not self._cloudpath.endswith('/') and not pattern.startswith('/') else '') +  pattern
        try:
            matches = await self._accessor.async_glob(glob_pattern)
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
        matches = self._accessor.find(path = self._cloudstr, maxdepth = maxdepth, withdirs = withdirs, detail = detail, prefix = pattern)
        if self.is_cloud:
            matches = [f'{self._prefix}://{m}' for m in matches]
        if not as_string:
            matches = [type(self)(m) for m in matches]
        return matches

    async def async_find(self, pattern: str = "*",  as_string: bool = False, maxdepth: int = None, withdirs: bool = None, detail: bool = False) -> Union[List[str], List[Type['CloudFileSystemPath']]]:
        """
        List all files below path. Like posix find command without conditions
        """
        matches = await self._accessor.async_find(path = self._cloudstr, maxdepth = maxdepth, withdirs = withdirs, detail = detail, prefix = pattern)
        if self.is_cloud:
            matches = [f'{self._prefix}://{m}' for m in matches]
        if not as_string:
            matches = [type(self)(m) for m in matches]
        return matches

    def rglob(self, pattern: str, as_path: bool = True) -> Iterable[Union[str, Type['CloudFileSystemPath']]]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        return self.glob(pattern = f'**/{pattern}', as_path = as_path)

    async def async_rglob(self, pattern: str) -> AsyncIterable[Union[str, Type['CloudFileSystemPath']]]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        return await self.async_glob(f'**/{pattern}')

    def cat(self, recursive: bool = False, on_error: str = 'raise', **kwargs):
        """
        Fetch paths’ contents
        Parameters
        recursive: bool
            If True, assume the path(s) are directories, and get all the contained files

        on_error“raise”, “omit”, “return”
            If raise, an underlying exception will be raised (converted to KeyError if the type is in self.missing_exceptions);
            if omit, keys with exception will simply not be included in the output; if “return”, all keys are included in the output,
            but the value will be bytes or an exception instance.

        kwargs: passed to cat_file
        """
        return self._accessor.cat(self._cloudstr, recursive = recursive, on_error = on_error, **kwargs)

    async def async_cat(self, recursive: bool = False, on_error: str = 'raise', **kwargs):
        """
        Fetch paths’ contents
        Parameters
        recursive: bool
            If True, assume the path(s) are directories, and get all the contained files

        on_error“raise”, “omit”, “return”
            If raise, an underlying exception will be raised (converted to KeyError if the type is in self.missing_exceptions);
            if omit, keys with exception will simply not be included in the output; if “return”, all keys are included in the output,
            but the value will be bytes or an exception instance.

        kwargs: passed to cat_file
        """
        return await self._accessor.async_cat(self._cloudstr, recursive = recursive, on_error = on_error, **kwargs)

    def cat_file(self, as_bytes: bool = False, start: int = None, end: int = None, **kwargs):
        """
        Parameters
        start, end: int
            Bytes limits of the read. If negative, backwards from end, like usual python slices. Either can be None for start or end of file, respectively

        kwargs: passed to ``open()``.
        """
        res = self._accessor.cat_file(self._cloudstr, start = start, end = end, **kwargs)
        if not as_bytes and isinstance(res, bytes): res = res.decode('UTF-8')
        return res

    async def async_cat_file(self, as_bytes: bool = False, start: int = None, end: int = None, **kwargs):
        """
        Parameters
        start, end: int
            Bytes limits of the read. If negative, backwards from end, like usual python slices. Either can be None for start or end of file, respectively

        kwargs: passed to ``open()``.
        """
        res = await self._accessor.async_cat_file(self._cloudstr, start = start, end = end, **kwargs)
        if not as_bytes and isinstance(res, bytes): res = res.decode('UTF-8')
        return res

    def pipe(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return self._accessor.pipe(self._cloudstr, value = value, **kwargs)

    async def async_pipe(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return await self._accessor.async_pipe(self._cloudstr, value = value, **kwargs)

    def pipe_file(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return self._accessor.pipe_file(self._cloudstr, value = value, **kwargs)

    async def async_pipe_file(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return await self._accessor.async_pipe_file(self._cloudstr, value = value, **kwargs)


    def absolute(self) -> Type['CloudFileSystemPath']:
        """Return an absolute version of this path.  This function works
        even if the path doesn't point to anything.
        No normalization is done, i.e. all '.' and '..' will be kept along.
        Use resolve() to get the canonical path to a file.
        """
        raise NotImplementedError


    def resolve(self, strict: bool = False) -> Type['CloudFileSystemPath']:
        """
        Make the path absolute, resolving all symlinks on the way and also
        normalizing it (for example turning slashes into backslashes under
        Windows).
        """
        s: Optional[str] = self._flavour.resolve(self, strict=strict)

        if s is None:
            self.stat()
            path = self.absolute()
            s = str(path)

        # Now we have no symlinks in the path, it's safe to normalize it.
        normed: str = self._flavour.pathmod.normpath(s)
        obj = self._from_parts((normed,), init=False)
        obj._init(template=self)
        return obj

    async def async_resolve(self, strict: bool = False) -> Type['CloudFileSystemPath']:
        """
        Make the path absolute, resolving all symlinks on the way and also
        normalizing it (for example turning slashes into backslashes under
        Windows).
        """
        s: Optional[str] = await self._flavour.async_resolve(self, strict=strict)

        if s is None:
            await self.async_stat()
            path = await self.absolute()
            s = str(path)

        # Now we have no symlinks in the path, it's safe to normalize it.
        normed: str = self._flavour.pathmod.normpath(s)
        obj = self._from_parts((normed,), init=False)
        obj._init(template=self)
        return obj

    def stat(self) -> stat_result:
        """
        Return the result of the stat() system call on this path, like
        os.stat() does.
        """
        return self._accessor.stat(self._cloudpath)

    async def async_stat(self) -> stat_result:
        """
        Return the result of the stat() system call on this path, like
        os.stat() does.
        """
        return await self._accessor.async_stat(self._cloudpath)

    def info(self) -> Dict[str, Union[str, int, float, datetime.datetime, datetime.timedelta, List[str], Any]]:
        """
        Return the result of the info() system call on this path, like
        """
        return self._accessor.info(self._cloudpath)

    async def async_info(self) -> Dict[str, Union[str, int, float, datetime.datetime, datetime.timedelta, List[str], Any]]:
        """
        Return the result of the info() system call on this path, like
        os.stat() does.
        """
        return await self._accessor.async_info(self._cloudpath)

    def size(self) -> int:
        """
        Return the size of the file, reported by os.path.getsize.
        """
        return self._accessor.size(self._cloudpath)

    async def async_size(self) -> int:
        """
        Return the size of the file, reported by os.path.getsize.
        """
        return await self._accessor.async_size(self._cloudpath)

    def bytesize(self) -> ByteSize:
        """
        Return the size of the file in bytes, reported by os.path.getsize().
        """
        return ByteSize.validate(self.size())
    
    async def async_bytesize(self) -> ByteSize:
        """
        Return the size of the file in bytes, reported by os.path.getsize().
        """
        return ByteSize.validate(await self.async_size())

    def lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        raise NotImplementedError

    async def async_lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        raise NotImplementedError

    def owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        raise NotImplementedError

    async def async_owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        raise NotImplementedError

    def group(self) -> str:
        """
        Return the group name of the file gid.
        """
        raise NotImplementedError

    async def async_group(self) -> str:
        """
        Return the group name of the file gid.
        """
        raise NotImplementedError

    def is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        return self._accessor.is_dir(self._cloudpath)


    async def async_is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        return await self._accessor.async_is_dir(self._cloudpath)

    def is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        raise NotImplementedError


    async def async_is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        raise NotImplementedError

    def is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        return self._accessor.is_file(self._cloudpath)

    async def async_is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        return await self._accessor.async_is_file(self._cloudpath)

    @staticmethod
    def _get_pathlike(path: 'FileLike') -> 'FileLike':
        """
        Returns the path of the file.
        """
        from fileio.lib.types import File
        return File(path)

    def copy(self, dest: 'FileLike', recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies the File to the Dir/File.
        """
        dest = self._get_pathlike(dest)
        if dest.is_dir() and self.is_file():
            dest = dest.joinpath(self.filename_)
        if dest.exists() and not overwrite and dest.is_file():
            if skip_errors: return dest
            raise exceptions.FileExistsError(f'File {dest._path} exists')
        if dest.is_cloud: self._accessor.copy(self._cloudpath, dest._cloudpath, recursive)
        else: self._accessor.get(self._cloudpath, dest._path, recursive)
        return dest

    async def async_copy(self, dest: 'FileLike', recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies the File to the Dir/File.
        """
        dest = self._get_pathlike(dest)
        if await dest.async_is_dir() and await self.async_is_file():
            dest = dest.joinpath(self.filename_)
        if await dest.async_exists() and not overwrite and await dest.async_is_file():
            if skip_errors: return dest
            raise exceptions.FileExistsError(f'File {dest._path} exists')
        if dest.is_cloud: await self._accessor.async_copy(self._cloudpath, dest._cloudpath, recursive = recursive)
        else: await self._accessor.async_get(self._cloudpath, dest.string, recursive = recursive)
        return dest

    def copy_file(self, dest: 'FileLike', recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies this File to the the Dest Path
        """
        dest = self._get_pathlike(dest)
        if dest.is_dir() and self.is_file():
            dest = dest.joinpath(self.filename_)
        if dest.exists() and not overwrite and dest.is_file():
            if skip_errors: return dest
            raise exceptions.FileExistsError(f'File {dest._path} exists')
        if dest.is_cloud: self._accessor.copy(self._cloudpath, dest._cloudpath, recursive)
        else: self._accessor.get(self._cloudpath, dest._path, recursive)
        return dest

    async def async_copy_file(self, dest: 'FileLike', recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies this File to the the Dest Path
        """
        dest = self._get_pathlike(dest)
        if await dest.async_is_dir() and await self.async_is_file():
            dest = dest.joinpath(self.filename_)
        if await dest.async_exists() and not overwrite and await dest.async_is_file():
            if skip_errors: return dest
            raise exceptions.FileExistsError(f'File {dest._path} exists')
        if dest.is_cloud: await self._accessor.async_copy(self._cloudpath, dest._cloudpath, recursive = recursive)
        else: await self._accessor.async_get(self._cloudpath, dest.string, recursive = recursive)
        return dest

    def put(self, src: 'FileLike', recursive: bool = False, callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copy file(s) from src to this FilePath
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.put(src.string, self._cloudpath, recursive=recursive, callback=callback, **kwargs)

    async def async_put(self, src: 'FileLike', recursive: bool = False, callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copy file(s) from src to this FilePath
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_put(src.string, self._cloudpath, recursive=recursive, callback=callback, **kwargs)

    def put_file(self, src: 'FileLike', callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copy single file to remote
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.put_file(src.string, self._cloudpath, callback=callback, **kwargs)

    async def async_put_file(self, src: 'FileLike', callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copy single file to remote
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_put_file(src.string, self._cloudpath, callback=callback, **kwargs)

    def get(self, dest: 'FileLike', recursive: bool = False, callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copy the remote file(s) to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.get(self._cloudpath, dest.string, recursive=recursive, callback=callback, **kwargs)

    async def async_get(self, dest: 'FileLike', recursive: bool = False, callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copy the remote file(s) to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_get(self._cloudpath, dest.string, recursive=recursive, callback=callback, **kwargs)

    def get_file(self, dest: 'FileLike', callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copies this file to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.get_file(self._cloudpath, dest.string, callback=callback, **kwargs)

    async def async_get_file(self, dest: 'FileLike', callback: Optional[Callable] = Callback(), **kwargs):
        """
        Copies this file to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_get_file(self._cloudpath, dest.string, callback=callback, **kwargs)


    def is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        return False if not self.exists() or not self.is_dir() else False
        #raise NotImplementedError


    async def async_is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not await self.async_exists() or not await self.async_is_dir(): return False
        return False
        #raise NotImplementedError


    def is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        return False
        #raise NotImplementedError

    async def async_is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        return False
        #raise NotImplementedError

    def is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        return False
        #raise NotImplementedError


    async def async_is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        return False

    def is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        return False


    async def async_is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        return False


    def is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        return False


    async def async_is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        return False


    def expanduser(self) -> Type['CloudFileSystemPath']:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = self._flavour.gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self

    async def async_expanduser(self) -> Type['CloudFileSystemPath']:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = await self._flavour.async_gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self

    def iterdir(self) -> Iterable[Type['CloudFileSystemPath']]:
        names = self._accessor.listdir(self)
        for name in names:
            if name in {'.', '..'}: continue
        yield self._make_child_relpath(name)

    async def async_iterdir(self) -> AsyncIterable[Type['CloudFileSystemPath']]:
        names = await self._accessor.async_listdir(self)
        for name in names:
            if name in {'.', '..'}: continue
        yield self._make_child_relpath(name)

    async def async_ls(
        self,
        recursive: bool = False,
        detail: Optional[bool] = False,
        versions: Optional[bool] = False,
        refresh: Optional[bool] = False,
        as_path: Optional[bool] = True,
        files_only: Optional[bool] = True,
        prettify: Optional[bool] = True,
        **kwargs,
    ) -> Union[List['CloudFileSystemPath'], List[Dict[str, Any]]]:
        """
        Return a list of the files in this directory.
        """
        
        ls_partial = functools.partial(self.afilesys._ls, detail=True, versions=versions, refresh=refresh)

        async def _ls(path: Dict[str, Any]):
            """
            Inner function to handle recursive ls
            """
            ps = []
            if path.get('type', path.get('StorageClass', '')).lower() == 'directory' \
                and recursive:
                new_paths = await ls_partial(path['Key'])
                results = await asyncio.gather(*[_ls(p) for p in new_paths])
                for result in results:
                    ps.extend(result)
                if not files_only: ps.append(path)
            if files_only and path.get('type', path.get('StorageClass', '')).lower() != 'directory':
                ps.append(path)
            return ps

        from pydantic.types import ByteSize
        from fileio.utils.pooler import async_map
        paths: List[Dict[str, Any]] = await ls_partial(self._cloudpath)
        all_paths = []
        async for path in async_map(_ls, paths):
            all_paths.extend(path)

        if detail: 
            for p in all_paths:
                p['Key'] = f'{self._prefix}://{p["Key"]}'
                if prettify: p['SizePretty'] = ByteSize.validate(p['Size']).human_readable()
            if as_path:
                p['File'] = type(self)(p['Key'])
            return all_paths
        final_paths = []
        for path in all_paths:
            if as_path: 
                path_str = f'{self._prefix}://{path["Key"]}'
                path = type(self)(path_str)
            final_paths.append(path)
        return final_paths
        

    def _raise_closed(self):
        raise ValueError("I/O operation on closed path")

    def _raise_open(self):
        raise ValueError("I/O operation on already open path")

    # We sort of assume that it will be used to open a file
    def __enter__(self):
        #if self._fileio: self._raise_open()
        #if not self._fileio:
        #    self._fileio = self.open()
        if self._closed: self._raise_closed()
        return self

    def __exit__(self, t, v, tb):
        self._closed = True

    async def __aenter__(self):
        if self._closed: self._raise_closed()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._closed = True

    """
    Other Methods
    """
    def url(self, **kwargs):
        return self._accessor.url(self._cloudpath, **kwargs)

    async def async_url(self, **kwargs):
        return await self._accessor.async_url(self._cloudpath, **kwargs)

    def setxattr(self, **kwargs):
        return self._accessor.setxattr(self._cloudpath, **kwargs)

    async def async_setxattr(self, **kwargs):
        return await self._accessor.async_setxattr(self._cloudpath, **kwargs)

    def cloze(self, **kwargs):
        if self._fileio:
            self._fileio.commit()
        return self._accessor.invalidate_cache(self._cloudpath)

    async def async_cloze(self, **kwargs):
        return await self._accessor.async_invalidate_cache(self._cloudpath)

    def get_checksum(
        self,
        method: str = 'md5',
        chunk_size: int = 1024,
        **kwargs
    ):
        """
        Creates the checksum for the file
        """
        hashmethod = getattr(hashlib, method)
        hasher = hashmethod()
        with self.open('rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                hasher.update(chunk)
        checksum = hasher.hexdigest()
        del hasher
        return checksum

    async def async_get_checksum(
        self,
        method: str = 'md5',
        chunk_size: int = 1024,
        **kwargs
    ):
        """
        Creates the checksum for the file
        """
        hashmethod = getattr(hashlib, method)
        hasher = hashmethod()
        async with self.async_open('rb') as f:
            if not self.is_cloud:
                for byte_block in await iter(lambda: f.read(chunk_size), b""):
                    hasher.update(byte_block)
            else:
                byte_block = await f.read(chunk_size)
                while byte_block:
                    hasher.update(byte_block)
                    byte_block = await f.read(chunk_size)
        checksum = hasher.hexdigest()
        del hasher
        return checksum


class CloudFileSystemPosixPath(PosixPath, CloudFileSystemPath, PureCloudFileSystemPosixPath):
    __slots__ = ()


class CloudFileSystemWindowsPath(WindowsPath, CloudFileSystemPath, PureCloudFileSystemWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("CloudFileSystemPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("CloudFileSystemPath.async_is_mount() is unsupported on this system")


os.PathLike.register(CloudFileSystemPurePath)
os.PathLike.register(CloudFileSystemPath)
os.PathLike.register(PureCloudFileSystemPosixPath)
os.PathLike.register(CloudFileSystemWindowsPath)
os.PathLike.register(CloudFileSystemPosixPath)
os.PathLike.register(PureCloudFileSystemWindowsPath)

def register_pathlike(pathz: List[Union[PosixPath, CloudFileSystemPath, WindowsPath, CloudFileSystemWindowsPath, CloudFileSystemPosixPath, PureCloudFileSystemWindowsPath, Any]]):
    for p in pathz:
        os.PathLike.register(p)

__all__ = (
    'ClassVar',
    'AccessorLike',
    'CloudFileSystemLike',
    # 'get_accessor',
    # 'get_cloud_filesystem',
    'CloudFileSystemPurePath',
    'PurePath',
    'PureCloudFileSystemPosixPath',
    'PureCloudFileSystemWindowsPath',
    'CloudFileSystemPath',
    'Path',
    '_pathz_windows_flavour',
    '_pathz_posix_flavour',
    'CloudFileSystemPosixPath',
    'CloudFileSystemWindowsPath',
    'register_pathlike',
    'FileSysManager',
)

