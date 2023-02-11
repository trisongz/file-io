

__all__ = (
    'Any',
    'AnyStr',
    'Dict',
    'Iterator',
    'List',
    'Optional',
    'Sequence',
    'Tuple',
    'Type',
    'TypeVar',
    'Union',
    'Protocol',
    'PathLike',
    'PathLikeCls',
    'T',
    'TupleOrList',
    'TreeDict',
    'Tree',
    'Tensor',
    'Dim',
    'Shape',
    'JsonValue',
    'JsonType',
    'Key',
    'KeySerializedExample',
    'ModuleType',
    'Final',
    'Literal',
    'TextMode',
    'BinaryMode',
    'FileMode',
    'NoneType',
    'aobject',
    'FileInfo',
    'BaseModel'
)

import os
import sys
import datetime
from typing import Any, AnyStr, Dict, Iterator, List, Optional, Sequence, Tuple, Type, TypeVar, Union, TYPE_CHECKING
from types import ModuleType
from pydantic import BaseModel

try:
    from typing import Protocol
except ImportError:
    import typing_extensions
    Protocol = typing_extensions.Protocol

if sys.version_info >= (3, 8):
    from typing import Final, Literal
else:
    from typing_extensions import Final, Literal


# Accept both `str` and `pathlib.Path`-like
PathLike = Union[str, os.PathLike]
PathLikeCls = (str, os.PathLike)    # Used in `isinstance`

T = TypeVar('T')

# Note: `TupleOrList` avoid abiguity from `Sequence` (`str` is `Sequence[str]`,
# `bytes` is `Sequence[int]`).
TupleOrList = Union[Tuple[T, ...], List[T]]

TreeDict = Union[T, Dict[str, 'TreeDict']]    # pytype: disable=not-supported-yet
Tree = Union[T, TupleOrList['Tree'], Dict[str, 'Tree']]    # pytype: disable=not-supported-yet

Tensor = Union[T, Any]

Dim = Optional[int]
Shape = TupleOrList[Dim]

JsonValue = Union[str, bool, int, float, None, List['JsonValue'], Dict[str, 'JsonValue']]
JsonType = Dict[str, JsonValue]

Key = Union[int, str, bytes]
KeySerializedExample = Tuple[Key, bytes]

TextMode = \
  Literal['r', 'w', 'a', 'x', 'r+', 'w+', 'a+', 'x+']
BinaryMode = \
  Literal['rb', 'wb', 'ab', 'xb', 'r+b', 'w+b', 'a+b', 'x+b']
FileMode = Union[TextMode, BinaryMode]

NoneType = type(None)

class aobject(object):
    """Inheriting this class allows you to define an async __init__.

    So you can create objects by doing something like `await MyClass(params)`
    """
    async def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        await instance.__init__(*args, **kwargs)
        return instance

    async def __init__(self, *args, **kwargs):
        pass

if TYPE_CHECKING:
    from fileio.core.types import FileLike

class FileInfo(BaseModel):
    size: Optional[int] = None
    etag: Optional[str] = None
    last_modified: Optional[datetime.datetime] = None
    checksum: Optional[str] = None
    path: Optional['FileLike'] = None

    class Config:
        allow_arbitrary_types = True

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        if d['path']: d['path'] = self.path.as_posix()
        return d

    def validate_info(self):
        """
        Ensures that the file info is valid.
        """
        if not self.path or not self.path.exists():
            raise ValueError('Path does not exist.')
        if self.checksum is None:
            from fileio.utils.ops import checksum_file
            self.checksum = checksum_file(self.path)
        if not all(self.size, self.etag, self.last_modified):
            info: Dict[str, Any] = self.path.info()
            self.size = info.get('size', info.get('Size', 0))
            self.etag = info.get('ETag') or 'none'
            self.last_modified = info.get('LastModified')
        
    async def async_validate_info(self):
        """
        Ensures that the file info is valid.
        """
        if not self.path or not await self.path.async_exists():
            raise ValueError('Path does not exist.')
        if self.checksum is None:
            from fileio.utils.ops import async_checksum_file
            self.checksum = await async_checksum_file(self.path)
        if not all(self.size, self.etag, self.last_modified):
            info: Dict[str, Any] = await self.path.async_info()
            self.size = info.get('size', info.get('Size', 0))
            self.etag = info.get('ETag') or 'none'
            self.last_modified = info.get('LastModified')

    @classmethod
    def get_info(cls, path: 'FileLike') -> 'FileInfo':
        """
        Fetches file info from the given path.
        """
        from fileio import File
        from fileio.utils.ops import checksum_file

        path = File(path)
        file_info: Dict[str, Any] = path.info()
        return cls(
            size = file_info.get('size', file_info.get('Size', 0)),
            etag = file_info.get('ETag') or 'none',
            last_modified = file_info.get('LastModified'),
            checksum = checksum_file(path = path),
            path = path
        )
    
    @classmethod
    async def async_get_info(cls, path: 'FileLike') -> 'FileInfo':
        """
        Fetches file info from the given path.
        """
        from fileio import File
        from fileio.utils.ops import async_checksum_file
        
        path = File(path)
        file_info: Dict[str, Any] = await path.async_info()
        return cls(
            size = file_info.get('size', file_info.get('Size', 0)),
            etag = file_info.get('ETag') or 'none',
            last_modified = file_info.get('LastModified'),
            checksum = await async_checksum_file(path = path),
            path = path
        )

    def __eq__(self, other):
        if isinstance(other, FileInfo):
            return self.checksum == other.checksum
        return False