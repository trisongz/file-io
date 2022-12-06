# This should only be imported if tf is available.


from tensorflow.python.lib.io.file_io import copy_v2 as copy
from tensorflow.python.lib.io.file_io import create_dir_v2 as mkdir
from tensorflow.python.lib.io.file_io import delete_file_v2 as remove
from tensorflow.python.lib.io.file_io import delete_recursively_v2 as rmtree
from tensorflow.python.lib.io.file_io import file_exists_v2 as exists
from tensorflow.python.lib.io.file_io import get_matching_files_v2 as glob
from tensorflow.python.lib.io.file_io import get_registered_schemes
from tensorflow.python.lib.io.file_io import is_directory_v2 as isdir
from tensorflow.python.lib.io.file_io import join
from tensorflow.python.lib.io.file_io import list_directory_v2 as listdir
from tensorflow.python.lib.io.file_io import recursive_create_dir_v2 as makedirs
from tensorflow.python.lib.io.file_io import rename_v2 as rename
from tensorflow.python.lib.io.file_io import stat_v2 as stat
from tensorflow.python.lib.io.file_io import walk_v2 as walk
from tensorflow.python.platform.gfile import GFile as _GFile
from tensorflow.python.lib.io.file_io import FileIO as _FileIO

from fileio.aiopath.wrap import func_as_method_coro, func_to_async_func

class GFile(_FileIO):
    async_close = func_to_async_func(_FileIO.close)
    async_flush = func_to_async_func(_FileIO.flush)
    async_read = func_to_async_func(_FileIO.read)
    async_next = func_to_async_func(_FileIO.next)
    async_readline = func_to_async_func(_FileIO.readline)
    async_readlines = func_to_async_func(_FileIO.readlines)
    async_seek = func_to_async_func(_FileIO.seek)
    async_seekable = func_to_async_func(_FileIO.seekable)
    async_size = func_to_async_func(_FileIO.size)
    async_tell = func_to_async_func(_FileIO.tell)
    async_write = func_to_async_func(_FileIO.write)

    # async def __aenter__(self, *args, **kwargs):
    #     return self
    
    # async def __aexit__(self, *args, **kwargs):
    #     await self.async_close()
    

class tfFS:

    """
    Tensorflow Wrapped FS
    """

    async_walk = func_to_async_func(walk)
    async_stat = func_to_async_func(stat)
    async_rename = func_to_async_func(rename)
    async_makedirs = func_as_method_coro(makedirs)
    async_listdir = func_as_method_coro(listdir)
    async_join = func_as_method_coro(join)
    async_is_dir = func_as_method_coro(isdir)
    async_isdir = func_as_method_coro(isdir)
    async_glob = func_to_async_func(glob)
    async_exists = func_as_method_coro(exists)
    async_rmdir = func_as_method_coro(rmtree)
    async_rmtree = func_as_method_coro(rmtree)
    async_rm = func_as_method_coro(remove)
    async_remove = func_as_method_coro(remove)
    async_mkdir = func_as_method_coro(mkdir)
    async_copy = func_as_method_coro(copy)
    async_create_dir = func_as_method_coro(makedirs)

    walk = walk
    stat = stat
    rename = rename
    makedirs = makedirs
    listdir = listdir
    join = join
    is_dir = isdir
    isdir = isdir
    glob = glob
    exists = exists
    rmdir = rmtree
    rmtree = rmtree
    rm = remove
    remove = remove
    mkdir = mkdir
    copy = copy

    get_registered_schemes = get_registered_schemes
    GFile = GFile

    @classmethod
    def is_file(cls, *args, **kwargs):
        return not cls.is_dir(*args, **kwargs)
    
    @staticmethod
    def open(*args, **kwargs):
        return GFile(*args, **kwargs)
    
    @staticmethod
    async def async_open(*args, **kwargs):
        return GFile(*args, **kwargs)




