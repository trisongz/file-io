## Put things together to create types 
import inspect
import pathlib
import tempfile

from enum import Enum
from fileio.core.generic import get_path, FileLike
from typing import Union, Any, TypeVar, List, Optional, Callable, Dict

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
        Union[str, pathlib.Path, FileLike, FastUploadFile, StarletteUploadFile, StarliteUploadFile, Any]
    ],
    List[str], 
    List[pathlib.Path], 
    List[FileLike], 
    List[FastUploadFile], 
    List[StarletteUploadFile], 
    List[StarliteUploadFile],
    List[Any]
)


class LoadMode(str, Enum):
    default = 'default'
    binary = 'binary'
    text = 'text'


def get_filelike(path: FileType) -> FileLike:
    #if isinstance(file, PathzLike): return file
    if hasattr(path, '_cloudstr'): return path
    if hasattr(path, 'as_posix'): return get_path(path.as_posix())
    if isinstance(path, str): return get_path(path)
    if hasattr(path, 'file') and hasattr(getattr(path, 'file'), 'name'): return get_path(path.file.name)
    return get_path(path.name) if hasattr(path, 'name') else path

from fileio.io import Json, Yaml, Dill

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
        delete: bool = False,
        **kwargs
    ):
        """
        Creates a new temporary file
        """
        f = tempfile.NamedTemporaryFile(*args, delete = delete, **kwargs)
        f.close()
        return get_path(f.name)


    @classmethod
    async def async_load_json(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Union[Dict[Any, Any], List[Any], Any]:
        _file = get_filelike(*args, **kwargs) if file is None else file
        return Json.loads(await _file.async_read_text())
    
    @classmethod
    async def async_load_yaml(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Union[Dict[Any, Any], List[Any], Any]:
        _file = get_filelike(*args, **kwargs) if file is None else file
        return Yaml.loads(await _file.async_read_text())
    
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
        return Dill.loads(await _file.async_read_bytes())
    
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
        return Json.loads(_file.read_text())
    
    @classmethod
    def load_yaml(
        cls, 
        *args, 
        file: Union[FileLike, Any] = None, 
        **kwargs
    ) -> Union[Dict[Any, Any], List[Any], Any]:
        _file = get_filelike(*args, **kwargs) if file is None else file
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
        

