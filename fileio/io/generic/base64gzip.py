import base64
import gzip

from typing import Union
from .base import BasePack


class Base64GZip(BasePack):
    encoding: str = 'utf-8'

    @classmethod
    def encode(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, **kwargs) -> str:
        if isinstance(data, str): data = data.encode(encoding = encoding)
        return base64.b64encode(
            gzip.compress(data, *args, **kwargs)
            ).decode(encoding = encoding)

    @classmethod
    def decode(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, **kwargs) -> str:
        if isinstance(data, str): data = data.encode(encoding = encoding)
        return gzip.decompress(
            base64.b64decode(data, *args, **kwargs)
        ).decode(encoding = encoding)

    @classmethod
    def dumps(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, **kwargs) -> str:
        return cls.encode(data, encoding = encoding, *args, **kwargs)
    
    @classmethod
    def loads(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, **kwargs) -> str:
        return cls.decode(data, encoding = encoding, *args, **kwargs)

