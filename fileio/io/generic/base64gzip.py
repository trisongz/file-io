import base64
import gzip

from typing import Union, Optional
from .base import BasePack


class Base64GZip(BasePack):
    encoding: str = 'utf-8'

    @classmethod
    def encode(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, as_bytes: Optional[bool] = False, **kwargs) -> Union[str, bytes]:
        if isinstance(data, str): data = data.encode(encoding = encoding)
        data = base64.b64encode(gzip.compress(data, *args, **kwargs))
        return data if as_bytes else data.decode(encoding = encoding)

    @classmethod
    def decode(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, as_bytes: Optional[bool] = False, **kwargs) -> Union[str, bytes]:
        if isinstance(data, str): data = data.encode(encoding = encoding)
        data = gzip.decompress(base64.b64decode(data, *args, **kwargs))
        return data if as_bytes else data.decode(encoding = encoding)

    @classmethod
    def dumps(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, as_bytes: Optional[bool] = False, **kwargs) -> Union[str, bytes]:
        return cls.encode(data, encoding = encoding, *args, as_bytes = as_bytes, **kwargs)
    
    @classmethod
    def loads(cls, data: Union[str, bytes], encoding: str = 'utf-8', *args, as_bytes: Optional[bool] = False, **kwargs) -> Union[str, bytes]:
        return cls.decode(data, encoding = encoding, *args, as_bytes = as_bytes, **kwargs)

