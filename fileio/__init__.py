from __future__ import absolute_import

import os
from fileio import types

from fileio.core.imports import pathlib
from fileio.core.generic import *

from fileio.core import flavours
from fileio.core import base
from fileio.core import generic

from fileio import providers

from fileio.utils import settings
from fileio.core.types import File, FileLike, FileType, FileListType

from fileio.types.etc import ParsedFile, PreparedFile

# from fileio.converters.base import BaseConverter
# PreparedFile.update_forward_refs()
# ParsedFile.update_forward_refs()