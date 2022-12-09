
import json
from typing import Dict, Any, Union, List, Type, Generator, Optional, TYPE_CHECKING

from fileio.io._json import ObjectEncoder

if TYPE_CHECKING:
    from fileio.core.types import FileLike

class JsonLines:

    @staticmethod
    def dumps(
        data: List[Dict[Any, Any]], 
        default: Dict[Any, Any] = None, 
        cls: Type[json.JSONEncoder] = ObjectEncoder, 
        **kwargs
    ) -> Generator[str, None, None]:
        """
        Iterates through data and dumps each item
        """
        for item in data:
            yield json.dumps(item, default = default, cls = cls, **kwargs)

    @staticmethod
    def dump(
        data: List[Dict[Any, Any]], 
        path: 'FileLike',
        mode: str = 'auto',
        newline: str = '\n',
        buffering: int = -1,
        encoding: str = 'utf-8',
        default: Dict[Any, Any] = None, 
        cls: Type[json.JSONEncoder] = ObjectEncoder, 
        **kwargs
    ) -> int:
        """
        Iterates through data and dumps each item to a new line in a file.
        mode = 'auto' will automatically determine to use append or write if the file exists or not.
        """
        from fileio import File, FileLike
        file: FileLike  = File(path)
        if mode == 'auto': mode = 'a' if file.exists() else 'w'
        n = 0
        with file.open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            for item in data:
                f.write(json.dumps(item, default = default, cls = cls, **kwargs))
                f.write(newline)
                n += 1
        return n
    
    @staticmethod
    async def async_dump(
        data: List[Dict[Any, Any]], 
        path: 'FileLike',
        mode: str = 'auto',
        newline: str = '\n',
        buffering: int = -1,
        encoding: str = 'utf-8',
        default: Dict[Any, Any] = None, 
        cls: Type[json.JSONEncoder] = ObjectEncoder, 
        **kwargs
    ) -> int:
        """
        Iterates through data and dumps each item to a new line in a file.
        mode = 'auto' will automatically determine to use append or write if the file exists or not.
        """
        from fileio import File, FileLike
        file: FileLike = File(path)
        if mode == 'auto': mode = 'a' if await file.async_exists() else 'w'
        n = 0
        async with file.async_open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            for item in data:
                await f.write(json.dumps(item, default = default, cls = cls, **kwargs))
                await f.write(newline)
                n += 1
        return n

    @staticmethod
    def load_data(
        data: Union[List[str], List[bytes], List[Any]],
        **kwargs
    ) -> Generator[Union[Dict, Any], None, None]:
        """
        Iterates through data and loads each item
        """
        for item in data:
            yield json.loads(item, **kwargs)

    @staticmethod
    def loads(
        data: Optional[Union[List[str], List[bytes], List[Any]]] = None,
        path: Optional['FileLike'] = None, 
        mode: str = 'r', 
        newline: str = '\n',
        buffering: int = -1,
        encoding: str = 'utf-8',
        **kwargs
    ) -> Generator[Union[Dict, Any], None, None]:
        """
        Load data from a file or list of data.
        """
        assert data is not None or path is not None, 'Either data or path must be provided.'
        if data is not None:
            yield from JsonLines.load_data(data, **kwargs)
        from fileio import File
        file = File(path)
        with file.open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            for line in f:
                yield json.loads(line, **kwargs)
    
    @staticmethod
    async def async_loads(
        data: Optional[Union[List[str], List[bytes], List[Any]]] = None,
        path: Optional['FileLike'] = None, 
        mode: str = 'r', 
        newline: str = '\n',
        buffering: int = -1,
        encoding: str = 'utf-8',
        **kwargs
    ) -> Generator[Union[Dict, Any], None, None]:
        """
        Load data from a file or list of data.
        """
        assert data is not None or path is not None, 'Either data or path must be provided.'
        if data is not None:
            yield JsonLines.load_data(data, **kwargs)
        from fileio import File
        file = File(path)
        async with file.async_open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            for line in f:
                yield json.loads(line, **kwargs)
