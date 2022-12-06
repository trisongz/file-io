from typing import Dict, Any, Union, List
from fileio.io._base import BasePack

try:
    import msgpack

    class MsgPack(BasePack):
        
        @classmethod
        def dumps(cls, obj: Dict[Any, Any], *args, **kwargs) -> Union[bytes, str]:
            return msgpack.packb(obj, *args, **kwargs)
        
        @classmethod
        def loads(cls, data: Union[str, bytes], *args, raw: bool = False, **kwargs) -> Any:
            return msgpack.unpackb(data, *args, raw = raw, **kwargs)


except ImportError:
    # Use Default Dill if not installed
    from fileio.io._pickle import Dill
    class MsgPack(Dill):
        pass
