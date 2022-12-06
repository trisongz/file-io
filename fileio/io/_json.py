
import json
import datetime
import dataclasses
import contextlib
from typing import Dict, Any, Union, List, Type

from fileio.io._base import BasePack

try:
    import numpy as np
except ImportError:
    np = None



def object_serializer(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: object_serializer(v) for k, v in obj.items()}

    if isinstance(obj, bytes):
        return obj.decode('utf-8')

    if isinstance(obj, (str, list, dict, int, float, bool, type(None))):
        return obj

    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)

    if hasattr(obj, 'dict'): # test for pydantic models
        return obj.dict()
    
    if hasattr(obj, 'get_secret_value'):
        return obj.get_secret_value()
    
    if hasattr(obj, 'as_posix'):
        return obj.as_posix()
    
    if hasattr(obj, "numpy"):  # Checks for TF tensors without needing the import
        return obj.numpy().tolist()
    
    if hasattr(obj, 'tolist'): # Checks for torch tensors without importing
        return obj.tolist()
    
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    
    if np is not None:
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
            return int(obj)
        
        if isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        
    else:
        # Try to convert to a primitive type
        with contextlib.suppress(Exception):
            return int(obj)
        with contextlib.suppress(Exception):
            return float(obj)

    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class ObjectEncoder(json.JSONEncoder):
    
    def default(self, obj: Any):   # pylint: disable=arguments-differ,method-hidden
        try:
            return object_serializer(obj)
        except Exception as e:
            return json.JSONEncoder.default(self, obj)

class Json:

    @staticmethod
    def dumps(obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, cls: Type[json.JSONEncoder] = ObjectEncoder, **kwargs) -> str:
        return json.dumps(obj, *args, default = default, cls = cls, **kwargs)

    @staticmethod
    def loads(data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
        return json.loads(data, *args, **kwargs)


try:
    import orjson
    class OrJson(BasePack):
        
        @classmethod
        def dumps(cls, obj: Dict[Any, Any], *args, default: Dict[Any, Any] = None, **kwargs) -> str:
            return orjson.dumps(obj, default=default, *args, **kwargs).decode()
        
        @classmethod
        def loads(cls, data: Union[str, bytes], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
            return orjson.loads(data, *args, **kwargs)


except ImportError:
    # Use Default JSON if not installed
    class OrJson(Json):
        pass


