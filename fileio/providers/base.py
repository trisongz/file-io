from __future__ import annotations


from ..base_imports import *
from ..flavours import _pathz_windows_flavour, _pathz_posix_flavour

from fileio.types import *
from .methods import _ASYNC_SYNTAX_MAPPING

if TYPE_CHECKING:
    from fileio.base import FilePath

URI_PREFIXES = ('gs://', 's3://', 'minio://', 'mio://', 's3compat://')
_URI_SCHEMES = frozenset(('gs', 's3', 'minio', 'mio', 's3compat'))
_URI_MAP_ROOT = {
    'gs://': '/gs/',
    's3://': '/s3/',
    'mio://': '/minio/',
    'minio://': '/minio/',
    's3compat://': '/s3compat/'
}
_PROVIDER_MAP = {
    'gs': 'GoogleCloudStorage',
    's3': 'AmazonS3',
    'mio': 'MinIO',
    'minio': 'MinIO',
    's3compat': 'S3Compatible'
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
    filelike = cast(IO[Union[str, bytes, os.PathLike]], filelike)
    file = AsyncFile(filelike)
    yield file
    await file.aclose()
