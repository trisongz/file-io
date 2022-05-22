

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
)

import os
from typing import Any, AnyStr, Dict, Iterator, List, Optional, Sequence, Tuple, Type, TypeVar, Union
from types import ModuleType

try:
    from typing import Protocol
except ImportError:
    import typing_extensions
    Protocol = typing_extensions.Protocol

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

