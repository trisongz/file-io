# handle posix stat stuff
import os
import sys
#import stat
import typing
import hashlib

#from datetime import datetime
#from urllib.parse import quote
from email.utils import formatdate
from mimetypes import guess_type as mimetypes_guess_type


try:
    # check if the Python version supports the parameter
    # using usedforsecurity=False to avoid an exception on FIPS systems
    # that reject usedforsecurity=True
    hashlib.md5(b"data", usedforsecurity=False)  # type: ignore[call-arg]

    def md5_hexdigest(
        data: bytes, *, usedforsecurity: bool = True
    ) -> str:  # pragma: no cover
        return hashlib.md5(  # type: ignore[call-arg]
            data, usedforsecurity=usedforsecurity
        ).hexdigest()

except TypeError:  # pragma: no cover

    def md5_hexdigest(data: bytes, *, usedforsecurity: bool = True) -> str:
        return hashlib.md5(data).hexdigest()

# Compatibility wrapper for `mimetypes.guess_type` to support `os.PathLike` on <py3.8
def guess_type(
    path: typing.Union[str, "os.PathLike[str]"], strict: bool = True
) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    if sys.version_info < (3, 8):  # pragma: no cover
        path = os.fspath(path)
    return mimetypes_guess_type(path, strict)



def get_file_info(path: typing.Union[str, "os.PathLike[str]"]) -> typing.Dict[str, typing.Any]:
    if sys.version_info < (3, 8):  # pragma: no cover
        path = os.fspath(path)
    stat_result = os.stat(path)
    last_modified = formatdate(stat_result.st_mtime, usegmt=True)
    etag_base = f"{str(stat_result.st_mtime)}-{str(stat_result.st_size)}"
    etag = md5_hexdigest(etag_base.encode(), usedforsecurity=False)
    return {
        "size": stat_result.st_size,
        "last-modified": last_modified,
        "etag": etag,
        "media-type": guess_type(path)[0] or "text/plain",
        "filename": os.path.basename(path),
    }