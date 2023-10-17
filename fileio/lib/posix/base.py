from __future__ import annotations

import anyio
from fileio.lib.core import *
from fileio.lib.flavours import _pathz_windows_flavour, _pathz_posix_flavour
from fileio.lib.posix.static import _ASYNC_SYNTAX_MAPPING

from fileio.types.common import *
from fileio.utils.logs import logger

if TYPE_CHECKING:
    from fileio.lib.base import FilePath
    from fsspec.spec import AbstractBufferedFile
    from fsspec.asyn import AsyncFileSystem, AbstractAsyncStreamedFile
    from fileio.lib.posix.filesys import CloudFileSystemLike, AccessorLike
    from _typeshed import OpenBinaryMode, OpenTextMode, ReadableBuffer, WriteableBuffer

GIT_PREFIXES = ('gh://', 'git://', 'hf://')
URI_PREFIXES = ('gs://', 's3://', 'az://', 'minio://', 'mio://', 's3c://', 'r2://', 'wsbi://') + GIT_PREFIXES
_URI_SCHEMES = frozenset(('gs', 's3', 'az', 'minio', 'mio', 's3c', 'r2', 'wsbi', 'az', 'gh', 'git', 'hf'))
_URI_MAP_ROOT = {
    'gs://': '/gs/',
    's3://': '/s3/',
    'az://': '/azure/',
    'mio://': '/minio/',
    'minio://': '/minio/',
    's3c://': '/s3c/',
    'gh://': '/github/',
    'git://': '/git/',
    'hf://': '/huggingface/',
    'r2://': '/r2/',
    'wsbi://': '/wasabi/',
}
_PROVIDER_MAP = {
    'gs': 'GoogleCloudStorage',
    's3': 'AmazonS3',
    'az': 'Azure',
    'mio': 'MinIO',
    'minio': 'MinIO',
    's3c': 'S3Compatible',
    'gh': 'Github',
    'git': 'Git',
    'hf': 'HuggingFace',
    'r2': 'CloudFlare',
}

Paths = Union['FilePath', Path, str]


def rewrite_async_syntax(obj, provider: str = 's3'):
    """
    Basically - we're rewriting all the fsspec's async method
    from _method to async_method for syntax
    """
    _names = _ASYNC_SYNTAX_MAPPING[provider]
    for attr in dir(obj):
        if attr.startswith('_') and not attr.startswith('__'):
            attr_val = getattr(obj, attr)
            if iscoroutinefunction(attr_val) and _names.get(attr):
                setattr(obj, _names[attr], attr_val)
    return obj

@asynccontextmanager
async def get_cloud_handle(name: Paths, mode: FileMode = 'r', buffering: int = -1, encoding: str | None = ENCODING, errors: str | None = ERRORS, newline: str | None = SEP) -> AsyncContextManager[Handle]:
    file: AsyncFile
    if 'b' in mode: file = await open_file(name, mode)
    else: file = await open_file(name, mode, encoding=encoding, errors=errors, newline=newline)
    yield file
    await file.aclose()


@asynccontextmanager
async def get_cloud_file(filelike: Paths) -> AsyncContextManager[Handle]:
    file: AsyncFile
    filelike = cast(IO[Union[str, bytes, os.PathLike, Any]], filelike)
    file = AsyncFile(filelike)
    yield file
    await file.aclose()

class AsyncFileS(AsyncFile):
    """
    Wraps the file object to provide a more intuitive interface
    """

    def __init__(self, fp: Union['AbstractAsyncStreamedFile', IO[AnyStr]], is_asyn: Optional[bool] = False) -> None:
        self._fp: Union['AbstractAsyncStreamedFile', Any] = fp
        self._is_asyn = is_asyn

    async def aclose(self) -> None:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.close):
                return await self._fp.close()
            return await anyio.to_thread.run_sync(self._fp.close)
        return await super().aclose()

    async def read(self, size: int = -1) -> AnyStr:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.read):
                return await self._fp.read(size)
            return await anyio.to_thread.run_sync(self._fp.read, size)
        return super().read(size)

    async def read1(self, size: int = -1) -> bytes:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.read):
                return await self._fp.read(size)
            return await anyio.to_thread.run_sync(self._fp.read, size)
        return await super().read1(size)

    async def readline(self) -> AnyStr:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.readline):
                return await self._fp.readline()
            return await anyio.to_thread.run_sync(self._fp.readline)
        return await super().readline()

    async def readlines(self) -> list[AnyStr]:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.readlines):
                return await self._fp.readlines()
            return await anyio.to_thread.run_sync(self._fp.readlines)
        return await super().readlines()
    
    async def readinto(self, b: WriteableBuffer) -> bytes:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.readinto):
                return await self._fp.readinto(b)
            return await anyio.to_thread.run_sync(self._fp.readinto, b)
        return await super().readinto(b)

    async def readinto1(self, b: WriteableBuffer) -> bytes:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.readinto1):
                return await self._fp.readinto1(b)
            return await anyio.to_thread.run_sync(self._fp.readinto1, b)
        return await super().readinto1(b)

    async def write(self, b: ReadableBuffer | str) -> int:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.write):
                return await self._fp.write(b)
            return await anyio.to_thread.run_sync(self._fp.write, b)
        return await super().write(b)

    async def writelines(self, lines: Iterable[ReadableBuffer] | Iterable[str]) -> None:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.writelines):
                return await self._fp.writelines(lines)
            return await anyio.to_thread.run_sync(self._fp.writelines, lines)
        return await super().writelines(lines)
    
    async def truncate(self, size: int | None = None) -> int:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.truncate):
                return await self._fp.truncate(size)
            return await anyio.to_thread.run_sync(self._fp.truncate, size)
        return await super().truncate(size)

    async def seek(self, offset: int, whence: int | None = os.SEEK_SET) -> int:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.seek):
                return await self._fp.seek(offset, whence)
            return await anyio.to_thread.run_sync(self._fp.seek, offset, whence)
        return await super().seek(offset, whence)

    async def tell(self) -> int:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.tell):
                return await self._fp.tell()
            return await anyio.to_thread.run_sync(self._fp.tell)
        return await super().tell()

    async def flush(self, *args) -> None:
        if self._is_asyn:
            if iscoroutinefunction(self._fp.flush):
                return await self._fp.flush(*args)
            return await anyio.to_thread.run_sync(self._fp.flush, *args)
        return await super().flush()



@asynccontextmanager
async def get_cloudfs_file(accessor: 'AccessorLike', path: Paths, mode: FileMode = 'rb', **kwargs) -> AsyncContextManager[Handle]:
    """
    Helper function to open a file from a filesystem
    """
    if 'wb' in mode and hasattr(accessor.async_filesys, 'open_async'):
        asyncfile: 'AbstractAsyncStreamedFile' = await accessor.async_filesys.open_async(path, mode, **kwargs)
        if not hasattr(asyncfile, '_closed'):
            setattr(asyncfile, '_closed', False)
        # logger.warning(f'asyncfile: {asyncfile} - {type(asyncfile)} - {asyncfile.closed}')
        filelike = cast(IO[Union[str, bytes, os.PathLike, Any]], asyncfile)
        # file = AsyncFileS(filelike, is_asyn = True)            
    else:
        syncfile: 'AbstractBufferedFile' = accessor.open(path, mode, **kwargs)
        filelike = cast(IO[Union[str, bytes, os.PathLike, Any]], syncfile)
    
    file = AsyncFileS(filelike, is_asyn = True)
    try:
        yield file
    finally:
        await file.aclose()

    # if 'rb' in mode and hasattr(accessor.async_filesys, 'open_async'):
    # if hasattr(accessor.async_filesys, 'open_async'):
    #     try:
    #         asyncfile: 'AbstractAsyncStreamedFile' = await accessor.async_filesys.open_async(path, mode, **kwargs)
    #         filelike = cast(IO[Union[str, bytes, os.PathLike, Any]], asyncfile)
    #         file = AsyncFileS(filelike, is_asyn = True)            
    #         print('YIELDING FILE', type(file), file)
    #         yield file

    #     finally:
    #         print('CLOSING FILE')
    #         await file.close()
    #     file: 'AbstractAsyncStreamedFile' = await accessor.async_filesys.open_async(path, mode, **kwargs)
    #     yield file
    #     await file.close()
    # else:
    #     syncfile: 'AbstractBufferedFile' = accessor.open(path, mode, **kwargs)
    #     filelike = cast(IO[Union[str, bytes, os.PathLike, Any]], syncfile)
    #     file = AsyncFile(filelike)
    #     yield file
    #     await file.aclose()

    # file: AsyncFile
    # file = AsyncFile(filesys)
    # yield file
    # if hasattr(file, 'aclose'):
    #     await file.aclose()
    # else:
    #     await file.close()
