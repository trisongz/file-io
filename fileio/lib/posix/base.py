from __future__ import annotations

from fileio.lib.core import *
from fileio.lib.flavours import _pathz_windows_flavour, _pathz_posix_flavour
from fileio.lib.posix.static import _ASYNC_SYNTAX_MAPPING

from fileio.types.common import *

if TYPE_CHECKING:
    from fileio.lib.base import FilePath
    from fsspec.spec import AbstractBufferedFile
    from fsspec.asyn import AsyncFileSystem, AbstractAsyncStreamedFile
    from fileio.lib.posix.filesys import CloudFileSystemLike, AccessorLike

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

@asynccontextmanager
async def get_cloudfs_file(accessor: 'AccessorLike', path: Paths, mode: FileMode = 'rb', **kwargs) -> AsyncContextManager[Handle]:
    """
    Helper function to open a file from a filesystem
    """
    print('USING ASYNC')
    if 'b' in mode and hasattr(accessor.async_filesys, 'open_async'):
        print('USING ASYNC STREAMED')
        try:
            file: 'AbstractAsyncStreamedFile' = await accessor.async_filesys.open_async(path, mode, **kwargs)
            print('YIELDING FILE', type(file), file)
            yield file
            # async with file as f:
            #     print('YIELDING FILE')
            #     yield f

        finally:
            print('CLOSING FILE')
            await file.close()
        # file: 'AbstractAsyncStreamedFile' = await accessor.async_filesys.open_async(path, mode, **kwargs)
        # yield file
        # await file.close()
    else:
        syncfile: 'AbstractBufferedFile' = accessor.open(path, mode, **kwargs)
        filelike = cast(IO[Union[str, bytes, os.PathLike, Any]], syncfile)
        file = AsyncFile(filelike)
        yield file
        await file.aclose()

    # file: AsyncFile
    # file = AsyncFile(filesys)
    # yield file
    # if hasattr(file, 'aclose'):
    #     await file.aclose()
    # else:
    #     await file.close()
