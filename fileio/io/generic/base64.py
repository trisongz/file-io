import base64 as _base64

from typing import Union
from .base import BasePack


class Base64(BasePack):
    encoding: str = 'utf-8'

    @classmethod
    def encode(cls, text: str, encoding: str = 'utf-8', *args, **kwargs) -> str:
        return _base64.b64encode(text.encode(encoding = encoding), *args, **kwargs).decode(encoding = encoding)

    @classmethod
    def decode(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, **kwargs) -> str:
        if isinstance(data, str): data = data.encode(encoding = encoding)
        return _base64.b64decode(data, *args, **kwargs).decode(encoding = encoding)

    @classmethod
    def dumps(cls, data: str, encoding: str = 'utf-8', *args, **kwargs) -> str:
        return cls.encode(data, encoding = encoding, *args, **kwargs)
    
    @classmethod
    def loads(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, **kwargs) -> str:
        return cls.decode(data, encoding = encoding, *args, **kwargs)

