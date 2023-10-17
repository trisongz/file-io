import csv as _csv
from typing import Dict, Any, Union, List, Sequence, Generator, TYPE_CHECKING
from .base import BasePack

if TYPE_CHECKING:
    from fileio.lib.types import FileLike


class Csv(BasePack):

    @classmethod
    def dumps(
        cls, 
        data: List[Dict[Any, Any]], 
        path: 'FileLike',
        mode: str = 'w',
        newline: str = '\n',
        buffering: int = -1,
        encoding: str = 'utf-8',
        fieldnames: Sequence = None, 
        delimiter: str = ',',
        quotechar: str = '"',
        doublequote: bool = True,
        skipinitialspace: bool = False,
        lineterminator: bool = '\n',
        write_header: bool = True,
        **kwargs
    ) -> 'FileLike':
        from fileio.lib.types import File
        file = File(path)
        with file.open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            fieldnames = fieldnames if fieldnames is not None else data[0].keys()
            writer = _csv.DictWriter(
                f, 
                fieldnames = fieldnames,
                delimiter = delimiter,
                quotechar = quotechar,
                doublequote = doublequote,
                skipinitialspace = skipinitialspace,
                lineterminator = lineterminator,
                **kwargs
            )
            if write_header: writer.writeheader()
            for item in data:
                writer.writerow(item)
        return file
        

    @classmethod
    def load(
        cls, 
        path: 'FileLike', 
        mode: str = 'r', 
        newline: str = '\n',
        buffering: int = -1,
        encoding: str = 'utf-8',
        fieldnames: Sequence = None, 
        delimiter: str = ',',
        quotechar: str = '"',
        doublequote: bool = True,
        skipinitialspace: bool = False,
        lineterminator: bool = '\n',
        **kwargs
    ) -> _csv.DictReader:
        from fileio.lib.types import File
        return _csv.DictReader(
            File(path).open(mode = mode, newline = newline, buffering = buffering, encoding = encoding), 
            fieldnames = fieldnames, 
            delimiter = delimiter,
            quotechar = quotechar,
            doublequote = doublequote,
            skipinitialspace = skipinitialspace,
            lineterminator = lineterminator,
            **kwargs
        )
    
    @classmethod
    def loads(
        cls, 
        path: 'FileLike', 
        mode: str = 'r', 
        newline: str = '\n',
        buffering: int = -1,
        encoding: str = 'utf-8',
        fieldnames: Sequence = None, 
        delimiter: str = ',',
        quotechar: str = '"',
        doublequote: bool = True,
        skipinitialspace: bool = False,
        lineterminator: bool = '\n',
        **kwargs
    ) -> Generator[Dict, None, None]:
        from fileio.lib.types import File
        file = File(path)
        with file.open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            yield from _csv.DictReader(
                f, 
                fieldnames = fieldnames, 
                delimiter = delimiter,
                quotechar = quotechar,
                doublequote = doublequote,
                skipinitialspace = skipinitialspace,
                lineterminator = lineterminator,
                **kwargs
            )


