

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
from typing import Any, AnyStr, Dict, Iterator, List, Optional, Sequence, Tuple, Type, TypeVar, Union
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


class FileInfo(BaseModel):
    size: int = None
    etag: Optional[str] = None
    last_modified: Optional[datetime.datetime] = None
    checksum: Optional[str] = None

    class Config:
        allow_arbitrary_types = True

