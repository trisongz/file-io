from __future__ import annotations

import shutil
from stat import S_ISDIR, S_ISLNK, S_ISREG, S_ISSOCK, S_ISBLK, S_ISCHR, S_ISFIFO
from typing import ClassVar
from hashlib import md5

from .types import *
from .base_imports import *
from .aiopath.selectors import _make_selector
from .aiopath.scandir import EntryWrapper, scandir_async, _scandir_results
from .flavours import _pathz_windows_flavour, _pathz_posix_flavour


def scandir_sync(*args, **kwargs) -> Iterable[EntryWrapper]:
    results = _scandir_results(*args, **kwargs)
    yield from results

Paths = Union[Path, PathLike, str]
close = func_to_async_func(os.close)
sync_close = os.close




def generate_checksum(p: 'FilePath'):
    with p.open('rb') as f:
        file_hash = md5()
        chunk = f.read(8192)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(8192)
    return file_hash.hexdigest()

def calc_etag(inputfile: 'FilePath', partsize: int = 8388608):
    md5_digests = []
    with inputfile.open('rb') as f:
        for chunk in iter(lambda: f.read(partsize), b''):
            md5_digests.append(md5(chunk).digest())
    return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))


class _FileAccessor(NormalAccessor):
    # Sync methods
    stat = os.stat
    lstat = os.lstat
    open = os.open
    listdir = os.listdir
    chmod = os.chmod

    copy = shutil.copy
    copy_file = shutil.copyfile

    # Async Methods
    async_stat = func_as_method_coro(os.stat)
    async_lstat = func_as_method_coro(os.lstat)
    async_open = func_as_method_coro(os.open)
    async_listdir = func_as_method_coro(os.listdir)
    async_chmod = func_as_method_coro(os.chmod)

    async_copy = func_as_method_coro(shutil.copy)
    async_copy_file = func_as_method_coro(shutil.copyfile)

    if hasattr(NormalAccessor, 'lchmod'):
        lchmod = NormalAccessor.lchmod
        async_lchmod = method_as_method_coro(NormalAccessor.lchmod)

    mkdir = os.mkdir
    unlink = os.unlink

    async_mkdir = func_as_method_coro(os.mkdir)
    async_unlink = func_as_method_coro(os.unlink)

    if hasattr(NormalAccessor, 'link'):
        link = NormalAccessor.link
        async_link = method_as_method_coro(NormalAccessor.link)

    rmdir = os.rmdir
    rename = os.rename
    replace = os.replace
    symlink = staticmethod(NormalAccessor.symlink)
    utime = os.utime
    readlink = NormalAccessor.readlink
    remove = os.remove

    async_rmdir = func_as_method_coro(os.rmdir)
    async_rename = func_as_method_coro(os.rename)
    async_replace = func_as_method_coro(os.replace)
    async_symlink = staticmethod(method_as_method_coro(NormalAccessor.symlink))
    async_utime = func_as_method_coro(os.utime)
    async_readlink = method_as_method_coro(NormalAccessor.readlink)
    async_remove = func_as_method_coro(os.remove)


    def owner(self, path: str) -> str:
        try:
            import pwd
            stat = self.stat(path)
            return pwd.getpwuid(stat.st_uid).pw_name

        except ImportError:
            raise NotImplementedError("Path.owner() is unsupported on this system")

    async def async_owner(self, path: str) -> str:
        try:
            import pwd
            stat = await self.async_stat(path)
            return pwd.getpwuid(stat.st_uid).pw_name
        except ImportError: raise NotImplementedError("Path.owner() is unsupported on this system")

    def group(self, path: str) -> str:
        try:
            import grp
            stat = self.stat(path)
            return grp.getgrgid(stat.st_gid).gr_name

        except ImportError: raise NotImplementedError("Path.group() is unsupported on this system")

    async def async_group(self, path: str) -> str:
        try:
            import grp
            stat = await self.async_stat(path)
            return grp.getgrgid(stat.st_gid).gr_name

        except ImportError: raise NotImplementedError("Path.group() is unsupported on this system")

    def scandir(self, *args, **kwargs) -> Iterable[EntryWrapper]:
        yield from scandir_sync(*args, **kwargs)

    async def async_scandir(self, *args, **kwargs) -> AsyncIterable[EntryWrapper]:
        async for entry in scandir_async(*args, **kwargs):
            yield entry

_pathz_accessor = _FileAccessor()


class FilePurePath(PurePath):
    _prefix: str = None
    _provider: str = None
    _win_pathz: ClassVar = 'PureFileWindowsPath'
    _posix_pathz: ClassVar = 'PureFilePosixPath'

    def _init(self, template: Optional[PurePath] = None):
        self._accessor = _pathz_accessor

    def __new__(cls, *args):
        if cls is FilePurePath or issubclass(cls, FilePurePath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        return cls._from_parts(args)


class PureFilePosixPath(FilePurePath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    __slots__ = ()


class PureFileWindowsPath(FilePurePath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    __slots__ = ()


class FilePath(Path, FilePurePath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor = _pathz_accessor
    _prefix = None
    _provider = None
    _win_pathz: ClassVar = 'FileWindowsPath'
    _posix_pathz: ClassVar = 'FilePosixPath'

    def _init(self, template: Optional[FilePath] = None):
        self._accessor = _pathz_accessor
        self._closed = False
        self._fileio = None

    def __new__(cls, *args, **kwargs):
        #if cls is FilePath: cls = FileWindowsPath if os.name == 'nt' else FilePosixPath
        if cls is FilePath or issubclass(cls, FilePath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(args, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self

    @property
    def _path(self) -> str:
        return self._cloudstr if self.is_cloud else str(self)
    
    @property
    def checksum(self):
        return generate_checksum(self)
    
    @property
    def etag(self):
        return calc_etag(self)

    @property
    def _cloudpath(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        return None
    
    @property
    def _bucket(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        return None
    
    @property
    def _bucketstr(self) -> Optional[str]:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        return None
    
    @property
    def _pathkeys(self) -> Optional[str]:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        return None
    
    @property
    def _cloudstr(self) -> Optional[str]:
        """
        Reconstructs the proper cloud URI
        """
        return None
    
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
        if self.is_file(): return self.parts[-1]
        return ''

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
    def is_cloud(self) -> bool:
        return False
    
    @property
    def is_pathz(self) -> bool:
        return True

    @property
    def exists_(self) -> bool:
        return self.exists()
    
    @property
    def is_file_(self) -> bool:
        return self.is_file()
    
    @property
    def is_dir_(self) -> bool:
        return self.is_dir()
    
    @property
    def home_(self) -> FilePath:
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
    async def async_home_(self) -> FilePath:
        return await self.async_home()
    
    @staticmethod
    def _get_pathlike(path: PathLike):
        """
        Returns the path of the file.
        """
        from lazy.io.pathz_v2 import get_path
        return get_path(path)
    
    def open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        if 'b' in mode:
            return io.open(self, mode = mode, buffering = buffering, opener=self._opener)
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)

    
    def async_open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if 'b' in mode:
            return get_handle(self._path, mode = mode, buffering = buffering)
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)
    
    def reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)
    
    def async_reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)
    
    def appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)
    
    def async_appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)
    
    def writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)
    
    def async_writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)

    def read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS) -> str:
        with self.open('r', encoding=encoding, errors=errors) as file:
            return file.read()

    async def async_read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS) -> str:
        async with self.async_open('r', encoding=encoding, errors=errors) as file:
            return await file.read()

    def read_bytes(self) -> bytes:
        with self.open('rb') as file:
            return file.read()

    async def async_read_bytes(self) -> bytes:
        async with self.async_open('rb') as file:
            return await file.read()

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
        with self.open(mode='w', encoding=encoding, errors=errors, newline=newline) as f:
            return f.write(data)

    async def async_write_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        async with self.async_open(mode='w', encoding=encoding, errors=errors, newline=newline) as f:
            return await f.write(data)

    def readlink(self) -> FilePath:
        """
        Return the path to which the symbolic link points.
        """
        path: str = self._accessor.readlink(self)
        obj = self._from_parts((path,), init=False)
        obj._init(template=self)
        return obj

    async def async_readlink(self) -> FilePath:
        """
        Return the path to which the symbolic link points.
        """
        path: str = await self._accessor.async_readlink(self)
        obj = self._from_parts((path,), init=False)
        obj._init(template=self)
        return obj

    def _opener(self, name, flags, mode=0o666):
        # A stub for the opener argument to built-in open()
        return self._accessor.open(self, flags, mode)
    
    def _async_opener(self, name, flags, mode=0o666):
        # A stub for the opener argument to built-in open()
        return self._accessor.async_open(self, flags, mode)

    def _raw_open(self, flags: int, mode: int = 0o777) -> int:
        """
        Open the file pointed by this path and return a file descriptor,
        as os.open() does.
        """
        if self._closed: self._raise_closed()
        return self._accessor.open(self, flags, mode)

    async def _async_raw_open(self, flags: int, mode: int = 0o777) -> int:
        """
        Open the file pointed by this path and return a file descriptor,
        as os.open() does.
        """
        return await self._accessor.async_open(self, flags, mode)

    def touch(self, mode: int = 0o666, exist_ok: bool = True):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: self._accessor.utime(self, None)
            # Avoid exception chaining
            except OSError: pass
            else: return

        flags: int = os.O_CREAT | os.O_WRONLY
        if not exist_ok: flags |= os.O_EXCL
        fd = self._raw_open(flags, mode)
        sync_close(fd)

    async def async_touch(self, mode: int = 0o666, exist_ok: bool = True):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: await self._accessor.async_utime(self, None)
            # Avoid exception chaining
            except OSError: pass
            else: return

        flags: int = os.O_CREAT | os.O_WRONLY
        if not exist_ok: flags |= os.O_EXCL
        fd = await self._async_raw_open(flags, mode)
        await close(fd)

    def mkdir(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: self._accessor.mkdir(self, mode)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            self.parent.mkdir(parents=True, exist_ok=True)
            self.mkdir(mode, parents=False, exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not self.is_dir(): raise

    async def async_mkdir(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: await self._accessor.async_mkdir(self, mode)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            await self.parent.async_mkdir(parents=True, exist_ok=True)
            await self.async_mkdir(mode, parents=False, exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not await self.async_is_dir(): raise

    async def chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        self._accessor.chmod(self, mode)

    async def async_chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        await self._accessor.async_chmod(self, mode)

    def lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        self._accessor.lchmod(self, mode)

    async def async_lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        await self._accessor.async_lchmod(self, mode)

    def unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: self._accessor.unlink(self)
        except FileNotFoundError:
            if not missing_ok: raise

    async def async_unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: await self._accessor.async_unlink(self)
        except FileNotFoundError:
            if not missing_ok: raise

    def copy(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copies the File to the Dir/File.
        """
        dest: 'FilePath' = self._get_pathlike(dest)
        if dest.is_dir() and self.is_file():
            dest = dest.joinpath(self.filename_)
        
        if dest.exists() and not overwrite and dest.is_file():
            if skip_errors: return dest
            raise Exception(f'File {dest._path} exists')

        if not dest.is_cloud:
            self._accessor.copy(self._path, dest._path, **kwargs)
            return dest
        dest._accessor.put(self._path, dest._path, recursive)
        return dest
    
    async def async_copy(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        dest: 'FilePath' = self._get_pathlike(dest)
        if await dest.async_is_dir() and self.async_is_file():
            dest = dest.joinpath(self.filename_)
        
        if await dest.async_exists() and not overwrite and await dest.async_is_file():
            if skip_errors: return dest
            raise Exception(f'File {dest._path} exists')

        if not dest.is_cloud:
            await self._accessor.async_copy(self._path, dest._path, **kwargs)
            return dest
        await dest._accessor.async_put(self._path, dest._path, recursive)
        return dest
    
    def copy_file(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        dest: 'FilePath' = self._get_pathlike(dest)
        if dest.is_dir() and self.is_file():
            dest = dest.joinpath(self.filename_)
        
        if dest.exists() and not overwrite and dest.is_file():
            if skip_errors: return dest
            raise Exception(f'File {dest._path} exists')

        if not dest.is_cloud:
            self._accessor.copy_file(self._path, dest._path, **kwargs)
            return dest
        dest._accessor.put_file(self._path, dest._path, recursive)
        return dest

    async def async_copy_file(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        dest: 'FilePath' = self._get_pathlike(dest)
        if await dest.async_is_dir() and self.async_is_file():
            dest = dest.joinpath(self.filename_)
        
        if await dest.async_exists() and not overwrite and await dest.async_is_file():
            if skip_errors: return dest
            raise Exception(f'File {dest._path} exists')

        if not dest.is_cloud:
            await self._accessor.async_copy_file(self._path, dest._path, **kwargs)
            return dest
        await dest._accessor.async_put_file(self._path, dest._path, recursive)
        return dest

    def rm(self, **kwargs):
        """
        Remove this file or dir
        """
        if self.is_dir: return self.rmdir(**kwargs)
        
        return self._accessor.remove(self)
    
    async def async_rm(self, **kwargs):
        """
        Remove this file or dir
        """
        if self.is_dir: return await self.async_rmdir(**kwargs)
        await self._accessor.async_remove(self)

    def rm_file(self, **kwargs):
        """
        Remove this file 
        """
        
        self._accessor.remove(self)
    
    async def async_rm_file(self, **kwargs):
        """
        Remove this file 
        """
        
        return await self._accessor.async_remove(self)

    def rmdir(self, force: bool = False, recursive: bool = True, skip_errors: bool = True):
        """
        Remove this directory.  The directory must be empty.
        """
        
        self._accessor.rmdir(self)

    async def async_rmdir(self):
        """
        Remove this directory.  The directory must be empty.
        """
        await self._accessor.async_rmdir(self)

    def cat(self, as_bytes: bool = False, **kwargs):
        """
        Fetch paths’ contents
        """
        return self.read_bytes() if as_bytes else self.read_text()
    
    async def async_cat(self, as_bytes: bool = False, **kwargs):
        """
        Fetch paths’ contents
        """
        if as_bytes: return await self.async_read_bytes()
        return await self.async_read_text()
    
    def cat_file(self, as_bytes: bool = False, **kwargs):
        """
        """
        return self.cat(as_bytes, **kwargs)
    
    async def async_cat_file(self, as_bytes: bool = False, **kwargs):
        """
        Parameters
        start, end: int
            Bytes limits of the read. If negative, backwards from end, like usual python slices. Either can be None for start or end of file, respectively

        kwargs: passed to ``open()``.
        """
        return await self.async_cat(as_bytes, **kwargs)


    def pipe(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return self.write_bytes(value, **kwargs)

    async def async_pipe(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return await self.async_write_bytes(value, **kwargs)

    def pipe_file(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return self.write_bytes(value, **kwargs)

    async def async_pipe_file(self, value: Union[bytes, str], **kwargs):
        """
        Put value into path

        (counterpart to cat)
        """
        if not isinstance(value, bytes): value = value.encode('UTF-8')
        return await self.async_write_bytes(value, **kwargs)

    def link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        self._accessor.link_to(self, target)
    
    async def async_link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        await self._accessor.async_link_to(self, target)

    def rename(self, target: Union[str, FilePath]) -> FilePath:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.rename(self, target)
        return type(self)(target)
    
    async def async_rename(self, target: Union[str, FilePath]) -> FilePath:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_rename(self, target)
        return type(self)(target)

    def replace(self, target: str) -> FilePath:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.replace(self, target)
        return type(self)(target)
    
    async def async_replace(self, target: str) -> FilePath:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_replace(self, target)
        return type(self)(target)

    def symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        self._accessor.symlink(target, self, target_is_directory)
    
    async def async_symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        await self._accessor.async_symlink(target, self, target_is_directory)

    def exists(self) -> bool:
        """
        Whether this path exists.
        """
        try: self.stat()
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
        return True

    async def async_exists(self) -> bool:
        """
        Whether this path exists.
        """
        try: await self.async_stat()
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
        return True

    @classmethod
    def cwd(cls: type) -> str:
        """Return a new path pointing to the current working directory
        (as returned by os.getcwd()).
        """
        cwd: str = os.getcwd()
        return cls(cwd)

    @classmethod
    def home(cls: type) -> FilePath:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        homedir: str = cls()._flavour.gethomedir(None)
        return cls(homedir)

    @classmethod
    async def async_home(cls: type) -> FilePath:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        coro = cls()._flavour.async_gethomedir(None)
        homedir: str = await coro
        return cls(homedir)

    def samefile(self, other_path: Union[FilePath, Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = FilePath(other_path)
        if isinstance(other_path, FilePath):
            try: other_st = other_path.stat()
            except AttributeError: other_st = self._accessor.stat(other_path)

        else:
            try: other_st = other_path.stat()
            except AttributeError: other_st = other_path._accessor.stat(other_path)
        return os.path.samestat(self.stat(), other_st)

    async def async_samefile(self, other_path: Union[FilePath, Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = FilePath(other_path)
        if isinstance(other_path, FilePath):
            try: other_st = await other_path.async_stat()
            except AttributeError: other_st = await self._accessor.async_stat(other_path)

        else:
            try: other_st = await to_thread(other_path.stat)
            except AttributeError: other_st = await to_thread(other_path._accessor.stat, other_path)

        return os.path.samestat(await self.async_stat(),other_st)

    def iterdir(self) -> Iterable[FilePath]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        for name in self._accessor.listdir(self):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)

    async def async_iterdir(self) -> AsyncIterable[FilePath]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        for name in await self._accessor.async_listdir(self):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)

    def glob(self, pattern: str) -> Iterable[FilePath]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))

        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        selector = _sync_make_selector(tuple(pattern_parts), self._flavour)
        yield from selector.select_from(self)

    async def async_glob(self, pattern: str) -> AsyncIterable[FilePath]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))

        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        selector = _make_selector(tuple(pattern_parts), self._flavour)
        async for p in selector.select_from(self):
            yield p

    def rglob(self, pattern: str) -> Iterable[FilePath]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))

        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        parts = ("**", *pattern_parts)
        selector = _sync_make_selector(parts, self._flavour)
        yield from selector.select_from(self)

    async def async_rglob(self, pattern: str) -> AsyncIterable[FilePath]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        parts = ("**", *pattern_parts)
        selector = _make_selector(parts, self._flavour)
        async for p in selector.select_from(self):
            yield p

    def absolute(self) -> FilePath:
        """Return an absolute version of this path.  This function works
        even if the path doesn't point to anything.
        No normalization is done, i.e. all '.' and '..' will be kept along.
        Use resolve() to get the canonical path to a file.
        """
        if self.is_absolute(): return self
        obj = self._from_parts([os.getcwd()] + self._parts, init=False)
        obj._init(template=self)
        return obj

    def resolve(self, strict: bool = False) -> FilePath:
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

    async def async_resolve(self, strict: bool = False) -> FilePath:
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
        return self._accessor.stat(self)
    
    async def async_stat(self) -> stat_result:
        """
        Return the result of the stat() system call on this path, like
        os.stat() does.
        """
        return await self._accessor.async_stat(self)

    def lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        return self._accessor.lstat(self)
    
    async def async_lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        return await self._accessor.async_lstat(self)

    def owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        return self._accessor.owner(self)
    
    async def async_owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        return await self._accessor.async_owner(self)

    def group(self) -> str:
        """
        Return the group name of the file gid.
        """
        return self._accessor.group(self)
    
    async def async_group(self) -> str:
        """
        Return the group name of the file gid.
        """
        return await self._accessor.async_group(self)

    def is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        try:
            stat = self.stat()
            return S_ISDIR(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False
    
    async def async_is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        try:
            stat = await self.async_stat()
            return S_ISDIR(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        try:
            lstat = self.lstat()
            return S_ISLNK(lstat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False
    
    async def async_is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        try:
            lstat = await self.async_lstat()
            return S_ISLNK(lstat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        try:
            stat = self.stat()
            return S_ISREG(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    async def async_is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        try:
            stat = await self.async_stat()
            return S_ISREG(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not self.exists() or not self.is_dir(): return False
        
        try:
            parent_stat = self.parent.stat()
            parent_dev = parent_stat.st_dev
        except OSError: return False

        stat = self.stat()
        dev = stat.st_dev
        if dev != parent_dev: return True
        ino = stat.st_ino
        parent_ino = parent_stat.st_ino
        return ino == parent_ino

    async def async_is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not await self.async_exists() or not await self.async_is_dir(): return False
        
        try:
            parent_stat = await self.parent.async_stat()
            parent_dev = parent_stat.st_dev
        except OSError: return False

        stat = await self.async_stat()
        dev = stat.st_dev
        if dev != parent_dev: return True
        ino = stat.st_ino
        parent_ino = parent_stat.st_ino
        return ino == parent_ino

    def is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        try:
            stat = self.stat()
            return S_ISBLK(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    async def async_is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        try:
            stat = await self.async_stat()
            return S_ISBLK(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        try:
            stat = self.stat()
            return S_ISCHR(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
    
    async def async_is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        try:
            stat = await self.stat()
            return S_ISCHR(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    def is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        try:
            stat = self.stat()
            return S_ISFIFO(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    async def async_is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        try:
            stat = await self.async_stat()
            return S_ISFIFO(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    def is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        try:
            stat = self.stat()
            return S_ISSOCK(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
    
    async def async_is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        try:
            stat = await self.async_stat()
            return S_ISSOCK(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    def expanduser(self) -> FilePath:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = self._flavour.gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self
    
    async def async_expanduser(self) -> FilePath:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = await self._flavour.async_gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self

    def iterdir(self) -> Iterable[FilePath]:
        names = self._accessor.listdir(self)
        for name in names:
            if name in {'.', '..'}: continue
        yield self._make_child_relpath(name)
    
    async def async_iterdir(self) -> AsyncIterable[FilePath]:
        names = await self._accessor.async_listdir(self)
        for name in names:
            if name in {'.', '..'}: continue
        yield self._make_child_relpath(name)

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


class FilePosixPath(PosixPath, FilePath, PureFilePosixPath):
    __slots__ = ()


class FileWindowsPath(WindowsPath, FilePath, PureFileWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("FilePath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("FilePath.async_is_mount() is unsupported on this system")


os.PathLike.register(FilePurePath)
os.PathLike.register(FilePath)
os.PathLike.register(PureFilePosixPath)
os.PathLike.register(FileWindowsPath)
os.PathLike.register(FilePosixPath)
os.PathLike.register(PureFileWindowsPath)


__all__ = (
    'FilePurePath',
    'FilePath',
    'PureFilePosixPath',
    'FileWindowsPath',
    'FilePosixPath',
    'PureFileWindowsPath',
)
