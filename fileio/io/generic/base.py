from typing import Dict, Any, Union, List

class BasePack:

    @classmethod
    def dumps(cls, obj, *args, **kwargs):
        raise NotImplementedError
    
    @classmethod
    def loads(cls, bin, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def serialize(cls, obj, *args, **kwargs):
        return cls.dumps(obj, *args, **kwargs)
    
    @classmethod
    def deserialize(cls, bin, *args, **kwargs):
        return cls.loads(bin, *args, **kwargs)