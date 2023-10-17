from typing import Dict
from .base import BasePack
from .base64 import Base64
from .base64gzip import Base64GZip
from ._json import Json, OrJson
from .pickle import Pickle, Dill
from .msgpack import MsgPack
from .generator import Generate
from .text import Text
from .yaml import Yaml
from .csv import Csv
from .tsv import Tsv
from .jsonlines import JsonLines

Serializers: Dict[str, BasePack] = {
    'pickle': Pickle,
    'dill': Dill,
    'json': Json,
    'orjson': OrJson,
    'base64': Base64,
    'bgzip': Base64GZip,
    'msgpack': MsgPack,
    'yaml': Yaml,
    'text': Text,
    'csv': Csv,
    'tsv': Tsv,
    'jsonlines': JsonLines,
    
}

