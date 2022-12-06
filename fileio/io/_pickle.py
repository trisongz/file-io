import sys
import pickle
if sys.version_info.minor < 8:
    try:
        import pickle5 as pickle
    except ImportError:
        pass

import dill
from typing import Dict, Any, Union, List
from ._base import BasePack


class DefaultProtocols:
    default: int = 4
    pickle: int = pickle.HIGHEST_PROTOCOL
    dill: int = dill.HIGHEST_PROTOCOL

class Pickle(BasePack):

    @classmethod
    def dumps(cls, obj: Any, protocol: int = DefaultProtocols.pickle, *args, **kwargs) -> bytes:
        return pickle.dumps(obj, protocol = protocol, *args, **kwargs)

    @classmethod
    def loads(cls, data: bytes, *args, **kwargs) -> object:
        return pickle.loads(data, *args, **kwargs)


class Dill(BasePack):

    @classmethod
    def dumps(cls, obj: Any, protocol: int = DefaultProtocols.dill, *args, **kwargs) -> bytes:
        return dill.dumps(obj, protocol = protocol, *args, **kwargs)

    @classmethod
    def loads(cls, data: bytes, *args, **kwargs) -> object:
        return dill.loads(data, *args, **kwargs)
