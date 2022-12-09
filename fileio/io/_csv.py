import csv
from typing import Dict, Any, Union, List, Sequence, Generator, TYPE_CHECKING
from fileio.io._base import BasePack

if TYPE_CHECKING:
    from fileio.core.types import FileLike


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
        from fileio import File
        file = File(path)
        with file.open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            fieldnames = fieldnames if fieldnames is not None else data[0].keys()
            writer = csv.DictWriter(
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
    ) -> csv.DictReader:
        from fileio import File
        return csv.DictReader(
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
        from fileio import File
        file = File(path)
        with file.open(mode = mode, newline = newline, buffering = buffering, encoding = encoding) as f:
            yield from csv.DictReader(
                f, 
                fieldnames = fieldnames, 
                delimiter = delimiter,
                quotechar = quotechar,
                doublequote = doublequote,
                skipinitialspace = skipinitialspace,
                lineterminator = lineterminator,
                **kwargs
            )


