from enum import Enum

class LoadMode(str, Enum):
    default = 'default'
    binary = 'binary'
    text = 'text'
