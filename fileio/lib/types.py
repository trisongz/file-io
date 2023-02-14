"""
Core Types for the Library
"""

import os
import inspect
import pathlib
import tempfile
import atexit

from fileio.lib.core import pathlib
from fileio.lib.base import *

from fileio.providers.gcs import *
from fileio.providers.s3 import *
from fileio.providers.minio import *
from fileio.providers.s3c import *

from fileio.lib.apis import (
    FastUploadFile,
    StarletteUploadFile,
    StarliteUploadFile,
)

from fileio.types.options import LoadMode
from fileio.utils.pooler import ThreadPooler
from typing import Union, Any, TypeVar, List, Optional, Callable, Dict, Type, Tuple, TYPE_CHECKING


FileLike = Union[
    Type[FilePurePath],
    Type[FilePath],
    Type[PureFilePosixPath],
    Type[FileWindowsPath],
    Type[FilePosixPath],
    Type[PureFileWindowsPath],
    
    Type[FileGSPurePath],
    Type[FileGSPath],
    Type[PureFileGSPosixPath],
    Type[FileGSWindowsPath],
    Type[FileGSPosixPath],
    Type[PureFileGSWindowsPath],
    
    Type[FileS3PurePath],
    Type[FileS3Path],
    Type[PureFileS3PosixPath],
    Type[FileS3WindowsPath],
    Type[FileS3PosixPath],
    Type[PureFileS3WindowsPath],

    Type[FileMinioPurePath], 
    Type[FileMinioPath], 
    Type[PureFileMinioPosixPath], 
    Type[FileMinioWindowsPath], 
    Type[FileMinioPosixPath], 
    Type[PureFileMinioWindowsPath],

    Type[FileS3CPurePath],
    Type[FileS3CPath],
    Type[PureFileS3CPosixPath],
    Type[FileS3CWindowsPath],
    Type[FileS3CPosixPath],
    Type[PureFileS3CWindowsPath],

]

_PATHLIKE_CLS: Tuple[FileLike, ...] = (
    FilePurePath,
    FilePath,
    PureFilePosixPath,
    FileWindowsPath,
    FilePosixPath,
    PureFileWindowsPath,
    
    FileGSPurePath,
    FileGSPath,
    PureFileGSPosixPath,
    FileGSWindowsPath,
    FileGSPosixPath,
    PureFileGSWindowsPath,
    
    FileS3PurePath,
    FileS3Path,
    PureFileS3PosixPath,
    FileS3WindowsPath,
    FileS3PosixPath,
    PureFileS3WindowsPath,

    FileMinioPurePath, 
    FileMinioPath, 
    PureFileMinioPosixPath, 
    FileMinioWindowsPath, 
    FileMinioPosixPath, 
    PureFileMinioWindowsPath,

    FileS3CPurePath,
    FileS3CPath,
    PureFileS3CPosixPath,
    FileS3CWindowsPath,
    FileS3CPosixPath,
    PureFileS3CWindowsPath,
)

FileSysLike = Union[
    Type[AWSFileSystem],
    Type[GCPFileSystem],
    Type[MinioFileSystem],
    Type[S3CFileSystem],
]

PathLike = Union[str, os.PathLike, FileLike]

FileType = TypeVar(
    'FileType', 
    str, 
    pathlib.Path, 
    FileLike, 
    FastUploadFile, 
    StarletteUploadFile, 
    StarliteUploadFile,
    Any
)

FileListType = TypeVar(
    'FileListType', 
    List[
        Union[
            str, 
            pathlib.Path, 
            FileLike, 
            FastUploadFile, 
            StarletteUploadFile, 
            StarliteUploadFile, 
            Any
        ]
    ],
    List[str], 
    List[pathlib.Path], 
    List[FileLike], 
    List[FastUploadFile], 
    List[StarletteUploadFile], 
    List[StarliteUploadFile],
    List[Any]
)


_PREFIXES_TO_CLS: Dict[str, FileLike] = {
    'gs://': FileGSPath,
    's3://': FileS3Path,
    #'s3a://': FileMinioPath,
    'mio://': FileMinioPath,
    'minio://': FileMinioPath,

    's3compat://': FileS3CPath,
    's3c://': FileS3CPath,
    #'minio://': cloud.PosixMinioPath,
    #'s3compat://': cloud.PosixS3CompatPath,
}


def as_path(path: PathLike) -> FileLike:
    """
    Given a path-like object, return a path-like object
    
    Create a generic `pathlib.Path`-like abstraction.
    Depending on the input (e.g. `gs://`, `github://`, `ResourcePath`,...), the
    system (Windows, Linux,...), the function will create the right pathlib-like
    abstraction.

    Args:
      path (PathLike): Pathlike object.
    
    Returns:
      A pathlib-like abstraction.
    """
    if isinstance(path, str):
        uri_splits = path.split('://', maxsplit=1)
        if len(uri_splits) > 1:    
            # str is URI (e.g. `gs://`, `github://`,...)
            return _PREFIXES_TO_CLS[f'{uri_splits[0]}://'](path)
        return FilePath(path)
    elif isinstance(path, _PATHLIKE_CLS):
        return path
    elif isinstance(path, os.PathLike):
        return FilePath(path)
    else: raise TypeError(f'Invalid path type: {path!r}')


def get_userhome(as_pathz: bool = True):
    h = os.path.expanduser('~')
    return as_path(h) if as_pathz else h

def get_cwd():
    return os.getcwd()


def resolve_relative(filepath: PathLike) -> str:
    """
    If the filepath is a relative path, convert it to an absolute path
    
    Args:
      filepath (PathLike): The path to the file you want to resolve.
    
    Returns:
      A string.
    """
    if not isinstance(filepath, str): filepath = filepath.as_posix()
    if '://' in filepath: return filepath
    if filepath.startswith('~'): filepath = filepath.replace('~', get_userhome(), 1)
    elif filepath.startswith('../'): filepath = filepath.replace('..', get_cwd(), 1)
    elif filepath.startswith('..'): filepath = filepath.replace('..', f'{pathlib.Path(get_cwd()).parent.parent.as_posix()}/', 1)
    elif filepath.startswith('./'): filepath = filepath.replace('.', get_cwd(), 1)
    elif filepath.startswith('.'): filepath = filepath.replace('.', f'{pathlib.Path(get_cwd()).parent.as_posix()}/', 1)
    return filepath

def get_path(filepath: PathLike, resolve: bool = False) -> FileLike:
    if resolve: filepath = resolve_relative(filepath)
    if isinstance(filepath, str): filepath = as_path(filepath)
    return filepath

def get_pathlike(filepath: PathLike, resolve: bool = False) -> FileLike:
    if resolve: filepath = resolve_relative(filepath)
    if isinstance(filepath, str): filepath = as_path(filepath)
    return filepath

to_path = get_path

def get_filelike(path: FileType) -> FileLike:
    #if isinstance(file, PathzLike): return file
    if hasattr(path, '_cloudstr'): return path
    if hasattr(path, 'as_posix'): return get_path(path.as_posix())
    if isinstance(path, str): return get_path(path)
    if hasattr(path, 'file') and hasattr(getattr(path, 'file'), 'name'): return get_path(path.file.name)
    return get_path(path.name) if hasattr(path, 'name') else path

class File:
    def __new__(
        cls, 
        *args, 
        load_file: Optional[bool] = False, 
        mode: LoadMode = 'default', 
        loader: Callable = None, 
        **kwargs
    ) -> Union[FileLike, Any]:

        _file = get_filelike(*args, **kwargs)
        if load_file: return cls.load_file(file = _file, mode = mode, loader = loader)
        return _file

    @classmethod
    def get_tempfile(
        cls,
        *args,
        suffix: Optional[str] = None, 
        prefix: Optional[str] = None,  
        dir: Optional[str] = None,
        delete: bool = False,
        delete_on_exit: Optional[bool] = True,
        **kwargs
    ) -> FileLike:
        """
        Creates a new temporary file
        """
        f: tempfile._TemporaryFileWrapper = tempfile.NamedTemporaryFile(*args, suffix = suffix, prefix = prefix, dir = dir, delete = delete, **kwargs)
        f.close()
        p = get_path(f.name)
        if delete_on_exit:
            atexit.register(p.unlink, missing_ok = True)
        return p
    
    @classmethod
    def get_tempdir(
        cls,
        suffix: Optional[str] = None, 
        prefix: Optional[str] = None,  
        dir: Optional[str] = None, 
        delete_on_exit: Optional[bool] = False,
        **kwargs
    ) -> FileLike:
        """
        Creates a new temporary directory
        """
        d = tempfile.TemporaryDirectory(suffix = suffix, prefix = prefix, dir = dir)
        p = get_path(d.name)
        if delete_on_exit:
            atexit.register(p.rmdir, missing_ok = True)
        return p


    @classmethod
    async def async_load_json(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Union[Dict[Any, Any], List[Any], Any]:
        _file = get_filelike(*args, **kwargs) if file is None else file
        from fileio.io import Json
        return await ThreadPooler.run_async(
            Json.loads,
            await _file.async_read_text()
        )
        # return Json.loads(await _file.async_read_text())
    
    @classmethod
    async def async_load_yaml(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Union[Dict[Any, Any], List[Any], Any]:
        _file = get_filelike(*args, **kwargs) if file is None else file
        from fileio.io import Yaml
        return await ThreadPooler.run_async(
            Yaml.loads,
            await _file.async_read_text()
        )
        # return Yaml.loads(await _file.async_read_text())
    
    @classmethod
    async def async_load_text(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> str:
        _file = get_filelike(*args, **kwargs) if file is None else file
        return await _file.async_read_text()
    
    @classmethod
    async def async_load_pickle(
        cls, 
        *args, file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        from fileio.io import Dill
        return await ThreadPooler.run_async(
            Dill.loads,
            await _file.async_read_bytes()
        )
        # return Dill.loads(await _file.async_read_bytes())
    
    @classmethod
    async def async_load_file(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        mode: LoadMode = 'default', 
        loader: Callable = None, 
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        if loader is not None:
            data = await _file.async_read_bytes() if mode == LoadMode.binary else await _file.async_read_text()
            if inspect.iscoroutinefunction(loader):
                return await loader(data)
            return loader(data)
        if _file.extension == '.json':
            return await cls.async_load_json(file = _file)
        if _file.extension in {'.yml', '.yaml'}:
            return await cls.async_load_yaml(file = _file)
        if _file.extension in {'.pickle', '.pkl'}:
            return await cls.async_load_pickle(file = _file)
        if _file.extension in {'.txt', '.text'}:
            return await cls.async_load_text(file = _file)
        raise ValueError(f'Unknown file extension: {_file.extension}')
    
    @classmethod
    def load_json(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Union[Dict[Any, Any], List[Any], Any]:
        _file = get_filelike(*args, **kwargs) if file is None else file
        from fileio.io import Json
        return Json.loads(_file.read_text())
    
    @classmethod
    def load_yaml(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Union[Dict[Any, Any], List[Any], Any]:
        _file = get_filelike(*args, **kwargs) if file is None else file
        from fileio.io import Yaml
        return Yaml.loads(_file.read_text())
    
    @classmethod
    def load_text(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> str:
        _file = get_filelike(*args, **kwargs) if file is None else file
        return _file.read_text()
    
    @classmethod
    def load_pickle(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        from fileio.io import Dill
        return Dill.loads(_file.read_bytes())
    
    @classmethod
    def load_file(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        mode: LoadMode = 'default', 
        loader: Callable = None, 
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        if loader is not None:
            data = _file.read_bytes() if mode == LoadMode.binary else _file.read_text()
            return loader(data)
        if _file.extension == '.json':
            return cls.load_json(file = _file)
        if _file.extension in {'.yml', '.yaml'}:
            return cls.load_yaml(file = _file)
        if _file.extension in {'.pickle', '.pkl'}:
            return cls.load_pickle(file = _file)
        if _file.extension in {'.txt', '.text'}:
            return cls.load_text(file = _file)
        raise ValueError(f'Unknown file extension: {_file.extension}')
        



__all__ = (
    'get_path', 
    'as_path', 
    'PathLike', 
    'FilePath', 
    'FileS3Path', 
    'FileGSPath', 
    'FileLike',
    'FileS3CPath',
)