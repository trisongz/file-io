from __future__ import absolute_import

from typing import Dict
from fileio.io._base import BasePack
from fileio.io._base64 import Base64
from fileio.io._base64gzip import Base64GZip
from fileio.io._json import Json, OrJson
from fileio.io._pickle import Pickle, Dill
from fileio.io._msgpack import MsgPack
from fileio.io._generator import Generate
from fileio.io._text import Text
from fileio.io._yaml import Yaml
from fileio.io._csv import Csv
from fileio.io._tsv import Tsv
from fileio.io._jsonlines import JsonLines

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
}

