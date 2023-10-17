"""
Imports for API Classes
"""

try:
    from fastapi import UploadFile as FastUploadFile
except ImportError:
    FastUploadFile = object

try:
    from starlette.datastructures import UploadFile as StarletteUploadFile
except ImportError:
    StarletteUploadFile = object

try:
    from starlite.datastructures import UploadFile as StarliteUploadFile
except ImportError:
    StarliteUploadFile = object


__all__ = [
    'FastUploadFile',
    'StarletteUploadFile',
    'StarliteUploadFile'
]