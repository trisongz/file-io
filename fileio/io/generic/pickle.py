import sys
import pickle as _pickle

if sys.version_info.minor < 8:
    try:
        import pickle5 as _pickle
    except ImportError:
        pass

try:
    import dill
except ImportError:
    dill = _pickle

from typing import Dict, Any, Union, List
from .base import BasePack


class DefaultProtocols:
    default: int = 4
    pickle: int = _pickle.HIGHEST_PROTOCOL
    dill: int = dill.HIGHEST_PROTOCOL

class Pickle(BasePack):

    @classmethod
    def dumps(cls, obj: Any, protocol: int = DefaultProtocols.pickle, *args, **kwargs) -> bytes:
        return _pickle.dumps(obj, protocol = protocol, *args, **kwargs)

    @classmethod
    def loads(cls, data: bytes, *args, **kwargs) -> object:
        return _pickle.loads(data, *args, **kwargs)


class Dill(BasePack):

    @classmethod
    def dumps(cls, obj: Any, protocol: int = DefaultProtocols.dill, *args, **kwargs) -> bytes:
        return dill.dumps(obj, protocol = protocol, *args, **kwargs)

    @classmethod
    def loads(cls, data: bytes, *args, **kwargs) -> object:
        return dill.loads(data, *args, **kwargs)
