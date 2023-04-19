"""
Core Types for the Library
"""

import os
import inspect
import pathlib
import tempfile
import atexit

from fileio.lib.core import pathlib, IterableAIOFile
from fileio.lib.base import *

from fileio.providers.gcs import *
from fileio.providers.s3 import *
from fileio.providers.minio import *
from fileio.providers.s3c import *
from fileio.providers.r2 import *
from fileio.providers.wasabi import *

from fileio.lib.apis import (
    FastUploadFile,
    StarletteUploadFile,
    StarliteUploadFile,
)
from fileio.types.classprops import lazyproperty
from fileio.types.options import LoadMode
from fileio.utils.logs import logger
from fileio.utils.pooler import ThreadPooler
from typing import Union, Any, TypeVar, List, Optional, Callable, Dict, Type, Tuple, TYPE_CHECKING

PathLikeT = Union[
    FilePurePath,
    FilePath,
    PureFilePosixPath,
    FileWindowsPath,
    FilePosixPath,
    PureFileWindowsPath,
    
    Type[FilePurePath],
    Type[FilePath],
    Type[PureFilePosixPath],
    Type[FileWindowsPath],
    Type[FilePosixPath],
    Type[PureFileWindowsPath]
]

GSPathLikeT = Union[
    Type[FileGSPurePath],
    Type[FileGSPath],
    Type[PureFileGSPosixPath],
    Type[FileGSWindowsPath],
    Type[FileGSPosixPath],
    Type[PureFileGSWindowsPath],
]

S3PathLikeT = Union[
    Type[FileS3PurePath],
    Type[FileS3Path],
    Type[PureFileS3PosixPath],
    Type[FileS3WindowsPath],
    Type[FileS3PosixPath],
    Type[PureFileS3WindowsPath],
]

MinioPathLikeT = Union[
    Type[FileMinioPurePath], 
    Type[FileMinioPath], 
    Type[PureFileMinioPosixPath], 
    Type[FileMinioWindowsPath], 
    Type[FileMinioPosixPath], 
    Type[PureFileMinioWindowsPath],
]

S3CPathLikeT = Union[
    Type[FileS3CPurePath],
    Type[FileS3CPath],
    Type[PureFileS3CPosixPath],
    Type[FileS3CWindowsPath],
    Type[FileS3CPosixPath],
    Type[PureFileS3CWindowsPath],
]


R2PathLikeT = Union[
    Type[FileR2PurePath],
    Type[FileR2Path],
    Type[PureFileR2PosixPath],
    Type[FileR2WindowsPath],
    Type[FileR2PosixPath],
    Type[PureFileR2WindowsPath],
]

WasabiPathLikeT = Union[
    Type[FileWasabiPurePath],
    Type[FileWasabiPath],
    Type[PureFileWasabiPosixPath],
    Type[FileWasabiWindowsPath],
    Type[FileWasabiPosixPath],
    Type[PureFileWasabiWindowsPath],
]


# FileLike = Union[
#     Type[FilePurePath],
#     Type[FilePath],
#     Type[PureFilePosixPath],
#     Type[FileWindowsPath],
#     Type[FilePosixPath],
#     Type[PureFileWindowsPath],
    
#     Type[FileGSPurePath],
#     Type[FileGSPath],
#     Type[PureFileGSPosixPath],
#     Type[FileGSWindowsPath],
#     Type[FileGSPosixPath],
#     Type[PureFileGSWindowsPath],
    
#     Type[FileS3PurePath],
#     Type[FileS3Path],
#     Type[PureFileS3PosixPath],
#     Type[FileS3WindowsPath],
#     Type[FileS3PosixPath],
#     Type[PureFileS3WindowsPath],

#     Type[FileMinioPurePath], 
#     Type[FileMinioPath], 
#     Type[PureFileMinioPosixPath], 
#     Type[FileMinioWindowsPath], 
#     Type[FileMinioPosixPath], 
#     Type[PureFileMinioWindowsPath],

#     Type[FileS3CPurePath],
#     Type[FileS3CPath],
#     Type[PureFileS3CPosixPath],
#     Type[FileS3CWindowsPath],
#     Type[FileS3CPosixPath],
#     Type[PureFileS3CWindowsPath],

# ]
FileLike = PathLikeT

# FileLike = Union[
#     PathLikeT,
#     GSPathLikeT,
#     S3PathLikeT,
#     MinioPathLikeT,
#     S3CPathLikeT,
#     R2PathLikeT,
#     WasabiPathLikeT,
# ]

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

    FileR2PurePath,
    FileR2Path,
    PureFileR2PosixPath,
    FileR2WindowsPath,
    FileR2PosixPath,

    FileWasabiPurePath,
    FileWasabiPath,
    PureFileWasabiPosixPath,
    FileWasabiWindowsPath,
    FileWasabiPosixPath,
)

FileSysLike = Union[
    Type[AWSFileSystem],
    Type[GCPFileSystem],
    Type[MinioFileSystem],
    Type[S3CFileSystem],
    Type[R2FileSystem],
    Type[WasabiFileSystem],
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
    'r2://': FileR2Path,
    'wsbi://': FileWasabiPath,
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
    async def async_load_csv(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        csv_args: Dict[str, Any] = None,
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        if csv_args is None: csv_args = {}
        from fileio.io import Csv
        return await ThreadPooler.run_async(
            Csv.load,
            _file,
            **csv_args
        )

    @classmethod
    async def async_load_tsv(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        tsv_args: Dict[str, Any] = None,
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        if tsv_args is None: tsv_args = {}
        from fileio.io import Tsv
        return await ThreadPooler.run_async(
            Tsv.load,
            _file,
            **tsv_args
        )

    
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
        if _file.extension in {'.csv'}:
            return await cls.async_load_csv(file = _file)
        if _file.extension in {'.tsv'}:
            return await cls.async_load_tsv(file = _file)
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
    def load_csv(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        csv_args: Dict[str, Any] = None,
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        if csv_args is None: csv_args = {}
        from fileio.io import Csv
        return Csv.load(_file, **csv_args)

    @classmethod
    def load_tsv(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        tsv_args: Dict[str, Any] = None,
        **kwargs
    ) -> Any:
        _file = get_filelike(*args, **kwargs) if file is None else file
        if tsv_args is None: tsv_args = {}
        from fileio.io import Tsv
        return Tsv.load(_file, **tsv_args)

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
        if _file.extension in {'.csv'}:
            return cls.load_csv(file = _file)
        if _file.extension in {'.tsv'}:
            return cls.load_tsv(file = _file)
        raise ValueError(f'Unknown file extension: {_file.extension}')
        


class StatelessFile:
    
    """
    Stateless file are written to a local file during transactions
    and then uploaded to the remote storage once the transaction is complete.
    """

    def __init__(
        self, 
        input_file:  Optional[FileLike] = None,
        output_file: Optional[FileLike] = None,
        overwrite: Optional[bool] = False,
        load_file: Optional[bool] = False, 
        mode: LoadMode = 'default', 
        loader: Callable = None, 
        enable_auto_filename: Optional[bool] = None,
        output_file_suffix: Optional[str] = None,
        **kwargs
    ):
        if input_file:
            input_file = get_filelike(input_file, **kwargs)
            if load_file: input_file = File.load_file(file = input_file, mode = mode, loader = loader)
        self.file: FileLike = input_file if input_file is not None else None
        self._enable_auto_filename = enable_auto_filename
        self._output_file_suffix = output_file_suffix
        
        self._temp_src_file = File.get_tempfile(delete_on_exit = False)
        self._temp_out_file = File.get_tempfile(delete_on_exit = False)

        # self.output_file = File(output_file) if output_file is not None else None
        self._prepare_output_file(output_file = output_file)
        self.overwrite = overwrite
        # self._write_ready: bool = False
        self._read_ready: bool = False
        self._ready: bool = self.output_file is not None
        self._closed: bool = False
        atexit.register(self._onexit)
    
    def _create_autofile(self, file: FileLike, suffix: Optional[str] = None, parent: Optional[FileLike] = None):
        """
        Creates an auto filename for the given file.
        """
        suffix = suffix or self._output_file_suffix or file.suffix
        file_dir: FileLike = parent or (file.parent if file.is_file() else file)
        for i in range(100):
            output_file = file_dir.joinpath(f'{file.stem}_v{str(i).zfill(3)}{suffix}')
            if not output_file.exists():
                break
        return output_file

    def _prepare_output_file(self, output_file: Optional[FileLike] = None):
        """
        Prepares the output file for writing.
        - designed to handle edge cases where the output_file param
          is relative, is a directory, or is a file that already exists.
        """
        self.output_file: FileLike = None
        if not output_file: 
            if self.file and self._enable_auto_filename:
                self.output_file = self._create_autofile(file = self.file)
            return
        output_file = File(output_file)
        if output_file.is_dir() and self._enable_auto_filename:
            self.output_file = self._create_autofile(file = (self.file or output_file), parent = output_file)

        # if output_file.exists() and self._enable_auto_filename:
        #     # we'll create iterated filenames until we find one that doesn't exist
        #     self.output_file = self._create_autofile(file = output_file)

        elif not output_file.is_file():
            if self.file and self._enable_auto_filename:
                self.output_file = self._create_autofile(file = self.file, parent = output_file)
            elif self.file:
                output_file = (
                    output_file.joinpath(
                        f'{self.file.stem}_output{self._output_file_suffix or self.file.suffix}'
                    )
                    if output_file.joinpath(self.file.stem + (self._output_file_suffix or self.file.suffix)).exists()
                    else output_file.joinpath(self.file.stem + (self._output_file_suffix or self.file.suffix))
                )
            else:
                self.output_file = output_file.joinpath(f'{self._temp_out_file.name}{".output" if self._output_file_suffix is None else self._output_file_suffix}')
        else:
            self.output_file = output_file
        # self._write_ready = True
    
    def _prepare_file(self):
        """
        Function to prepare the file for read/writing.
        """
        if not self._read_ready:
            if self.file:
                self._temp_src_file.write_bytes(self.file.read_bytes())
            self._read_ready = True
        
    async def _async_prepare_file(self):
        """
        Function to prepare the file for read/writing.
        """
        if not self._read_ready:
            if self.file:
                await self._temp_src_file.async_write_bytes(await self.file.async_read_bytes())
            self._read_ready = True

    def write_bytes(self, data, flush: Optional[bool] = False, **kwargs):
        self._temp_out_file.write_bytes(data)
        if flush: self.flush(**kwargs)

    async def async_write_bytes(self, data: bytes, flush: Optional[bool] = False, **kwargs):
        await self._temp_out_file.async_write_bytes(data)
        if flush: await self.async_flush(**kwargs)

    def write_text(self, data, flush: Optional[bool] = False, **kwargs):
        self._temp_out_file.write_text(data)
        if flush: self.flush(**kwargs)

    async def async_write_text(self, data: str, flush: Optional[bool] = False, **kwargs):
        await self._temp_out_file.async_write_text(data, **kwargs)
        if flush: await self.async_flush(**kwargs)

    def write(self, data: Union[str, bytes], mode: str = 'w', flush: Optional[bool] = False, **kwargs):
        with self._temp_out_file.open(mode = mode, **kwargs) as f:
            f.write(data)
        if flush: self.flush(**kwargs)
    
    async def async_write(self, data: Union[str, bytes], mode: str = 'w', flush: Optional[bool] = False, **kwargs):
        async with self._temp_out_file.async_open(mode = mode, **kwargs) as f:
            await f.write(data)
        if flush: await self.async_flush(**kwargs)
    
    def read_bytes(self):
        self._prepare_file()
        if self.file:
            return self._temp_src_file.read_bytes()
        return self._temp_out_file.read_bytes()
    
    async def async_read_bytes(self):
        await self._async_prepare_file()
        if self.file:
            return await self._temp_src_file.async_read_bytes()
        return await self._temp_out_file.async_read_bytes()
    
    def read_text(self):
        self._prepare_file()
        if self.file:
            return self._temp_src_file.read_text()
        return self._temp_out_file.read_text()
    
    async def async_read_text(self):
        await self._async_prepare_file()
        if self.file:
            return await self._temp_src_file.async_read_text()
        return await self._temp_out_file.async_read_text()

    def read(self, mode: str = 'r', **kwargs):
        self._prepare_file()
        if self.file:
            with self._temp_src_file.open(mode = mode, **kwargs) as f:
                return f.read()
        with self._temp_out_file.open(mode = mode, **kwargs) as f:
            return f.read()
        
    async def async_read(self, mode: str = 'r', **kwargs):
        await self._async_prepare_file()
        async with self._temp_src_file.async_open(mode = mode, **kwargs) as f:
            return await f.read()

    def open(self, mode: str = 'r', **kwargs):
        if 'r' in mode and self.file:
            self._prepare_file()
            return self._temp_src_file.open(mode = mode, **kwargs)
        return self._temp_out_file.open(mode = mode, **kwargs)

    def async_open(self, mode: str = 'r', **kwargs) -> IterableAIOFile:
        if 'r' in mode and self.file:
            self._prepare_file()
            # logger.info('Opening: tmp src')
            return self._temp_src_file.async_open(mode = mode, **kwargs)
        # logger.info('Opening: tmp out')
        return self._temp_out_file.async_open(mode = mode, **kwargs)

    @lazyproperty
    def text(self):
        """
        Returns the text of the temp src file
        """
        self._prepare_file()
        if self.file:
            return self._temp_src_file.read_text()
        return self._temp_out_file.read_text()
    
    @lazyproperty
    def output_text(self):
        """
        Returns the text of the temp out file
        """
        if self._closed:
            if self.output_file:
                return self.output_file.read_text()
            raise RuntimeError('Cannot read output text after closing the file, and no output file was provided.')
        return self._temp_out_file.read_text()

    @lazyproperty
    def bytes(self):
        """
        Returns the bytes of the temp src file
        """
        self._prepare_file()
        if self.file:
            return self._temp_src_file.read_bytes()
        return self._temp_out_file.read_bytes()
    
    @lazyproperty
    def output_bytes(self):
        """
        Returns the bytes of the temp out file
        """
        if self._closed:
            if self.output_file:
                return self.output_file.read_bytes()
            raise RuntimeError('Cannot read output bytes after closing the file, and no output file was provided.')
        return self._temp_out_file.read_bytes()

    @lazyproperty
    async def async_text(self):
        """
        Returns the text of the temp src file
        """
        await self._async_prepare_file()
        if self.file:
            return await self._temp_src_file.async_read_text()
        return await self._temp_out_file.async_read_text()
    
    @lazyproperty
    async def async_output_text(self):
        """
        Returns the text of the temp out file
        """
        if self._closed:
            if self.output_file:
                return await self.output_file.async_read_text()
            raise RuntimeError('Cannot read output text after closing the file, and no output file was provided.')
        return await self._temp_out_file.async_read_text()

    @lazyproperty
    async def async_bytes(self):
        """
        Returns the bytes of the temp src file
        """
        await self._async_prepare_file()
        if self.file:
            return await self._temp_src_file.async_read_bytes()
        return await self._temp_out_file.async_read_bytes()

    @lazyproperty
    async def async_output_bytes(self):
        """
        Returns the bytes of the temp out file
        """
        if self._closed:
            if self.output_file:
                return await self.output_file.async_read_bytes()
            raise RuntimeError('Cannot read output bytes after closing the file, and no output file was provided.')
        return await self._temp_out_file.async_read_bytes()

    @lazyproperty
    def path(self):
        """
        Returns the path of the temp src file
        """
        self._prepare_file()
        if self.file:
            return self._temp_src_file.as_posix()
        return self._temp_out_file.as_posix()
    
    @lazyproperty
    def input_path(self):
        """
        Returns the path of the temp src file
        """
        if self._closed:
            raise RuntimeError('File is closed')
        self._prepare_file()
        return self._temp_src_file.as_posix()
    
    @lazyproperty
    def read_path(self) -> str:
        """
        Returns the path of the temp src file
        """
        if self._closed:
            raise RuntimeError('File is closed')
        self._prepare_file()
        return self._temp_src_file.as_posix()
    
    @lazyproperty
    def write_path(self) -> str:
        """
        Returns the path of the temp output file
        """
        if self._closed:
            raise RuntimeError('File is closed')
        return self._temp_out_file.as_posix()

    @lazyproperty
    def source_path(self):
        """
        Returns the path of the temp src file
        """
        return self.file.as_posix() if self.file else self._temp_src_file.as_posix()

    @lazyproperty
    def target_path(self):
        """
        Returns the path of the target output file
        """
        if self.output_file is None:
            if self.file and self.file.exists():
                return (
                    self.file.as_posix()
                    if self.overwrite
                    else self._temp_out_file.as_posix()
                )
            return self.file.as_posix()
        return self.output_file.as_posix()
    

    @lazyproperty
    def target_file(self) -> 'FileLike':
        """
        Returns the path of the target output file
        """
        if self.output_file is None:
            if self.file and self.file.exists():
                return (
                    self.file
                    if self.overwrite
                    else self._temp_out_file
                )
            return self.file
        return self.output_file

    def __getattr__(self, name, default: Any = None):
        self._prepare_file()
        return getattr(self._temp_src_file, name, default)
    
    def __getitem__(self, key):
        self._prepare_file()
        return self._temp_src_file[key]
    
    def __call__(self, *args, **kwargs):
        self._prepare_file()
        return self._temp_src_file(*args, **kwargs)

    def flush(
        self, 
        overwrite: Optional[bool] = None, 
        output_file: Optional[FileLike] = None, 
        **kwargs
    ):
        """
        Flushes the file to the output file.
        """
        if self._closed: 
            logger.warning('File is closed. Cannot flush.')
            return
        overwrite = overwrite or self.overwrite
        output_file = output_file or self.output_file
        if output_file is not None:
            logger.info(f'Writing to output file: {output_file}')
            output_file.write_bytes(self._temp_out_file.read_bytes())
        elif self.file and (overwrite or not self.file.exists()):
            logger.info(f'Writing to input file: {self.file}')
            self.file.write_bytes(self._temp_out_file.read_bytes())
    
    async def async_flush(
        self, 
        overwrite: Optional[bool] = None, 
        output_file: Optional[FileLike] = None, 
        **kwargs
    ):
        """
        Flushes the file to the output file.
        """
        if self._closed: 
            logger.warning('File is closed. Cannot flush.')
            return
        overwrite = overwrite or self.overwrite
        output_file = output_file or self.output_file
        if output_file is not None:
            logger.info(f'Writing to output file: {output_file}')
            await output_file.async_write_bytes(await self._temp_out_file.async_read_bytes())
        elif self.file and (overwrite or not self.file.exists()):
            logger.info(f'Writing to input file: {self.file}')
            await self.file.async_write_bytes(await self._temp_out_file.async_read_bytes())


    def close(self, *args, overwrite: Optional[bool] = None, output_file: Optional[FileLike] = None, **kwargs):
        if self._closed:
            return
        if self._ready:
            self.flush(overwrite = overwrite, output_file = output_file, **kwargs)
            self._ready = False        
        self._temp_src_file.rm_file(missing_ok=True)
        self._temp_out_file.rm_file(missing_ok=True)
        self._closed = True
    
    async def async_close(self, *args, overwrite: Optional[bool] = None, output_file: Optional[FileLike] = None, **kwargs):
        if self._closed:
            return
        if self._ready:
            await self.async_flush(overwrite = overwrite, output_file = output_file, **kwargs)
            self._ready = False
        
        await self._temp_src_file.async_rm_file(missing_ok=True)
        await self._temp_out_file.async_rm_file(missing_ok=True)
        self._closed = True

    def __enter__(self):
        self._prepare_file()
        return self._temp_src_file
    
    def __exit__(self, *args):
        self.close()
    
    def _onexit(self, *args, **kwargs):
        try:
            self.close()
        except Exception as e:
            pass
            # logger.error(e)
    
    def dict(self, *args, **kwargs):
        # self.flush()
        return {
            'input_path': self.input_path,
            'output_path': self.target_path,
            'source_path': self.source_path,
        }


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