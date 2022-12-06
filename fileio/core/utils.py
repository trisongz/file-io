
import hashlib
from fileio.core.types import FileType, get_filelike

DEFAULT_CHUNK_SIZE = 1024

def checksum_file(
    path: FileType, 
    chunk_size: int = DEFAULT_CHUNK_SIZE
):
    """
    Takes the uploaded file from the request and performs
    a SHA256 checksum to determine the uniqueness of the file.
    """
    path = get_filelike(path)
    sha256_hash = hashlib.sha256()
    with path.open('rb') as f:
        for byte_block in iter(lambda: f.read(chunk_size), b""):
            sha256_hash.update(byte_block)
    checksum = sha256_hash.hexdigest()
    del sha256_hash
    return checksum

async def async_checksum_file(
    path: FileType, 
    chunk_size: int = DEFAULT_CHUNK_SIZE
):
    """
    [Async]
    Takes the uploaded file from the request and performs
    a SHA256 checksum to determine the uniqueness of the file.
    """
    path = get_filelike(path)
    sha256_hash = hashlib.sha256()
    async with path.async_open('rb') as f:
        for byte_block in await iter(lambda: f.read(chunk_size), b""):
            sha256_hash.update(byte_block)
    checksum = sha256_hash.hexdigest()
    del sha256_hash
    return checksum


def checksum_bytes(data: bytes):
    """
    Takes the bytes of a file and performs
    a SHA256 checksum to determine the uniqueness of the file.
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data)
    checksum = sha256_hash.hexdigest()
    del sha256_hash
    return checksum
