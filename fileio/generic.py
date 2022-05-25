import os
from .baselib import pathlib
from .base import *
from .providers.gs_gcp import *
from .providers.s3_aws import *
from .providers.s3_minio import *

from typing import List, Dict, Union, Type, Tuple

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
    Type[FileMinioPurePath], Type[FileMinioPath], Type[PureFileMinioPosixPath], Type[FileMinioWindowsPath], Type[FileMinioPosixPath], Type[PureFileMinioWindowsPath]

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
    FileMinioPurePath, FileMinioPath, PureFileMinioPosixPath, FileMinioWindowsPath, FileMinioPosixPath, PureFileMinioWindowsPath
)

FileSysLike = Union[
    Type[AWSFileSystem],
    Type[GCPFileSystem],
    Type[MinioFileSystem],
]

PathLike = Union[str, os.PathLike, FileLike]

_PREFIXES_TO_CLS: Dict[str, FileLike] = {
    'gs://': FileGSPath,
    's3://': FileS3Path,
    #'s3a://': FileMinioPath,
    'mio://': FileMinioPath,
    'minio://': FileMinioPath,
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
            return _PREFIXES_TO_CLS[uri_splits[0] + '://'](path)
        return FilePath(path)
    elif isinstance(path, _PATHLIKE_CLS):
        return path
    elif isinstance(path, os.PathLike):
        return FilePath(path)
    else: raise TypeError(f'Invalid path type: {path!r}')


def get_userhome(as_pathz: bool = True):
    h = os.path.expanduser('~')
    if as_pathz: return as_path(h)
    return h

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
    elif filepath.startswith('..'): filepath = filepath.replace('..', pathlib.Path(get_cwd()).parent.parent.as_posix() + '/', 1)
    elif filepath.startswith('./'): filepath = filepath.replace('.', get_cwd(), 1)
    elif filepath.startswith('.'): filepath = filepath.replace('.', pathlib.Path(get_cwd()).parent.as_posix() + '/', 1)
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

class File:
    def __new__(cls, *args, **kwargs) -> FileLike:
        return get_path(*args, **kwargs)

__all__ = (
    'get_path', 
    'as_path', 
    'PathLike', 
    'File',
    'FilePath', 
    'FileS3Path', 
    'FileGSPath', 
    'FileLike',
)