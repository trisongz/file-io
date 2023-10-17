"""
Additional File Helpers
"""
import os
import aiohttpx
import hashlib
import contextlib

from typing import Callable, Dict, Any, Tuple, Optional, TYPE_CHECKING
from .helpers import timed_cache

if TYPE_CHECKING:
    from fileio.lib.types import FileLike
    with contextlib.suppress(ImportError):
        from starlette.requests import Request
        from starlette.datastructures import UploadFile


def get_url_file_name(url: str):
    url = url.split("#")[0]
    url = url.split("?")[0]
    return os.path.basename(url)

@timed_cache(secs = 60 * 60 * 24 * 1)
def checksum_file(path: 'FileLike', chunk_size: Optional[int] = None) -> str:
    """
    Takes the uploaded file from the request and performs
    a SHA256 checksum to determine the uniqueness of the file.

    :param path: The path to the file
    :param chunk_size: The size of the chunks to read from the file (default is 64kb)
    """
    from fileio.lib.types import File
    if not chunk_size: 
        from fileio.utils.configs import get_fileio_settings
        chunk_size = get_fileio_settings().read_chunk_size
    path = File(path)
    sha256_hash = hashlib.sha256()
    with path.open('rb') as f:
        for byte_block in iter(lambda: f.read(chunk_size), b""):
            sha256_hash.update(byte_block)
    checksum = sha256_hash.hexdigest()
    del sha256_hash
    return checksum


@timed_cache(secs = 60 * 60 * 24 * 1)
async def async_checksum_file(path: 'FileLike', chunk_size: Optional[int] = None) -> str:
    """
    [Async]
    Takes the uploaded file from the request and performs
    a SHA256 checksum to determine the uniqueness of the file.

    :param path: The path to the file
    :param chunk_size: The size of the chunks to read from the file (default is 64kb)
    """
    from fileio.lib.types import File
    if not chunk_size: 
        from fileio.utils.configs import get_fileio_settings
        chunk_size = get_fileio_settings().read_chunk_size
    path = File(path)
    sha256_hash = hashlib.sha256()
    async with path.async_open('rb') as f:
        if not path.is_cloud:
            for byte_block in await iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(byte_block)
        else:
            byte_block = await f.read(chunk_size)
            while byte_block:
                sha256_hash.update(byte_block)
                byte_block = await f.read(chunk_size)
            
    checksum = sha256_hash.hexdigest()
    del sha256_hash
    return checksum

def fetch_file_from_url(
    url: str,
    path: Optional['FileLike'] = None,
    directory: Optional[str] = None,
    filename: Optional[str] = None,
    chunk_size: Optional[int] = None,
    request_kwargs: Optional[Dict[str, str]] = None,
    overwrite: Optional[bool] = False,
    **kwargs,
) -> 'FileLike':
    """
    Fetches a file from a URL and saves it to the specified path.
    """
    from fileio.lib.types import File

    assert '://' in url, f'Invalid URL: {url}'
    if not chunk_size: 
        from fileio.utils.configs import get_fileio_settings
        chunk_size = get_fileio_settings().url_chunk_size
    if not path and not filename and not directory:
        path = File.get_tempfile()
    elif filename or directory:
        path_dir = File(directory) if directory else File.get_tempdir()
        path_dir.mkdir()
        path = path_dir.joinpath(filename or get_url_file_name(url))
    else:
        path = File(path)
    if not overwrite and path.exists(): return path
    with path.open('wb') as f:
        client = aiohttpx.Client()
        request_kwargs = request_kwargs or {}
        with client.stream('GET', url, **request_kwargs) as resp:
            for chunk in resp.aiter_bytes(chunk_size = chunk_size):
                f.write(chunk)
    return path

async def async_fetch_file_from_url(
    url: str,
    path: Optional['FileLike'] = None,
    directory: Optional[str] = None,
    filename: Optional[str] = None,
    chunk_size: Optional[int] = None,
    request_kwargs: Optional[Dict[str, str]] = None,
    overwrite: Optional[bool] = False,
    **kwargs,
) -> 'FileLike':
    """
    Fetches a file from a URL and saves it to the specified path.
    """
    from fileio.lib.types import File

    assert '://' in url, f'Invalid URL: {url}'
    if not chunk_size: 
        from fileio.utils.configs import get_fileio_settings
        chunk_size = get_fileio_settings().url_chunk_size
    if not path and not filename and not directory:
        path = File.get_tempfile()
    elif filename or directory:
        path_dir = File(directory) if directory else File.get_tempdir()
        await path_dir.async_mkdir()
        path = path_dir.joinpath(filename or get_url_file_name(url))
    else:
        path = File(path)
    if not overwrite and await path.async_exists(): return path
    async with path.async_open('wb') as f:
        client = aiohttpx.Client()
        request_kwargs = request_kwargs or {}
        async with client.async_stream('GET', url, **request_kwargs) as resp:
            async for chunk in resp.aiter_bytes(chunk_size = chunk_size):
                await f.write(chunk)
    return path



try:
    from pydantic.json import ENCODERS_BY_TYPE
except ImportError:
    ENCODERS_BY_TYPE = None 

def register_pydantic_type(obj, t):
    """
    Registers the type with Pydantic's JSON encoder.
    """
    global ENCODERS_BY_TYPE
    if ENCODERS_BY_TYPE is None: return
    # print(f"Registering {obj} = {t} with Pydantic's JSON encoder.")
    ENCODERS_BY_TYPE[obj] = t