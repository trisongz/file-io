import abc
import os
import typing

from typing import Any, AnyStr, Dict, Iterator, List, Optional, Sequence, Tuple, Type, TypeVar, Union
from fileio.core.libs import TF_FUNC, JSON_FUNC, JSON_PARSER, PICKLE_FUNC, PT_FUNC

try:
    from typing import Protocol
except ImportError:
    import typing_extensions
    Protocol = typing_extensions.Protocol


# Accept both `str` and `pathlib.Path`-like
PathLike = Union[str, os.PathLike]
PathLikeCls = (str, os.PathLike)    # Used in `isinstance`

T = TypeVar('T')

# Note: `TupleOrList` avoid abiguity from `Sequence` (`str` is `Sequence[str]`,
# `bytes` is `Sequence[int]`).
TupleOrList = Union[Tuple[T, ...], List[T]]

TreeDict = Union[T, Dict[str, 'TreeDict']]    # pytype: disable=not-supported-yet
Tree = Union[T, TupleOrList['Tree'], Dict[str, 'Tree']]    # pytype: disable=not-supported-yet

Tensor = Union[T, Any]
if TF_FUNC: Tensor = Union[TF_FUNC.Tensor, TF_FUNC.SparseTensor, TF_FUNC.RaggedTensor]

Dim = Optional[int]
Shape = TupleOrList[Dim]

JsonValue = Union[str, bool, int, float, None, List['JsonValue'], Dict[str, 'JsonValue']]
Json = Dict[str, JsonValue]

# Types for the tfrecord example construction.
Key = Union[int, str, bytes]
KeySerializedExample = Tuple[Key, bytes]


class PurePath(Protocol):
    """Protocol for pathlib.PurePath-like API."""
    parts: Tuple[str, ...]
    drive: str
    root: str
    anchor: str
    name: str
    suffix: str
    suffixes: List[str]
    stem: str

    def __new__(cls: Type[T], *args: PathLike) -> T:
        raise NotImplementedError

    def __fspath__(self) -> str:
        raise NotImplementedError

    def __hash__(self) -> int:
        raise NotImplementedError

    def __lt__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __le__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __gt__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __ge__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __truediv__(self: T, key: PathLike) -> T:
        raise NotImplementedError

    def __rtruediv__(self: T, key: PathLike) -> T:
        raise NotImplementedError

    def __bytes__(self) -> bytes:
        raise NotImplementedError

    def as_posix(self) -> str:
        raise NotImplementedError

    def as_uri(self) -> str:
        raise NotImplementedError

    def is_absolute(self) -> bool:
        raise NotImplementedError

    def is_reserved(self) -> bool:
        raise NotImplementedError

    def match(self, path_pattern: str) -> bool:
        raise NotImplementedError

    def relative_to(self: T, *other: PathLike) -> T:
        raise NotImplementedError

    def with_name(self: T, name: str) -> T:
        raise NotImplementedError

    def with_suffix(self: T, suffix: str) -> T:
        raise NotImplementedError

    def joinpath(self: T, *other: PathLike) -> T:
        raise NotImplementedError
    
    def get_blob(self: T) -> T:
        raise NotImplementedError

    @property
    def parents(self: T) -> Sequence[T]:
        raise NotImplementedError

    @property
    def parent(self: T) -> T:
        raise NotImplementedError
    
    @property
    def absolute_parent(self: T) -> T:
        raise NotImplementedError
    
    @property
    def is_cloud(self: T) -> bool:
        raise NotImplementedError

    @property
    def is_gcs(self: T) -> bool:
        raise NotImplementedError
    
    @property
    def is_s3(self: T) -> bool:
        raise NotImplementedError
    
    @property
    def is_sb(self: T) -> bool:
        raise NotImplementedError
    
    @property
    def file_ext(self: T) -> T:
        return self.suffix[1:]
    
    @property
    def bucket_id(self: T) -> Optional[str]:
        raise NotImplementedError

    @property
    def blob_id(self) -> Optional[str]:
        raise NotImplementedError

    # py3.9 backport of PurePath.is_relative_to.
    def is_relative_to(self, *other: PathLike) -> bool:
        """Return True if the path is relative to another path or False."""
        try:
            self.relative_to(*other)
            return True
        except ValueError:
            return False


class ReadOnlyPath(PurePath, Protocol):
    """Protocol for read-only methods of pathlib.Path-like API.
    See [pathlib.Path](https://docs.python.org/3/library/pathlib.html)
    documentation.
    """

    def __new__(cls: Type[T], *args: PathLike) -> T:
        if cls not in (ReadOnlyPath, ReadWritePath):
            return super().__new__(cls, *args)

        from fileio.core import generic_path
        return generic_path.as_path(*args)

    @abc.abstractmethod
    def exists(self) -> bool:
        """Returns True if self exists."""

    @abc.abstractmethod
    def is_dir(self) -> bool:
        """Returns True if self is a dir."""

    def is_file(self) -> bool:
        """Returns True if self is a file."""
        return not self.is_dir()

    @abc.abstractmethod
    def iterdir(self: T) -> Iterator[T]:
        """Iterates over the directory."""

    @abc.abstractmethod
    def glob(self: T, pattern: str) -> Iterator[T]:
        """Yielding all matching files (of any kind)."""
        # Might be able to implement using `iterdir` (recursivelly for `rglob`).

    def rglob(self: T, pattern: str) -> Iterator[T]:
        """Yielding all matching files recursivelly (of any kind)."""
        return self.glob(f'**/{pattern}')

    def expanduser(self: T) -> T:
        """Returns a new path with expanded `~` and `~user` constructs."""
        if '~' not in self.parts:    # pytype: disable=attribute-error
            return self
        raise NotImplementedError

    @abc.abstractmethod
    def resolve(self: T, strict: bool = False) -> T:
        """Returns the absolute path."""

    @abc.abstractmethod
    def open(
            self,
            mode: str = 'r',
            encoding: Optional[str] = None,
            errors: Optional[str] = None,
            **kwargs: Any,
    ) -> typing.IO[AnyStr]:
        """Opens the file."""
    
    def read(self, mode='rb') -> bytes:
        with self.open(mode=mode) as f:
            return f.read()
    
    def readlines(self) -> List[str]:
        with self.open('r') as f:
            return f.readlines()

    def read_bytes(self) -> bytes:
        """Reads contents of self as bytes."""
        with self.open('rb') as f:
            return f.read()

    def read_text(self, encoding: Optional[str] = None) -> str:
        """Reads contents of self as bytes."""
        with self.open('r', encoding=encoding) as f:
            return f.read()
    
    def read_json(self, encoding: Optional[str] = None) -> Json:
        with self.open('r', encoding=encoding) as f:
            return JSON_FUNC.load(f)
    
    def read_pkl(self, mode: str = 'rb+', **kwargs):
        with self.open(mode=mode) as f:
            return PICKLE_FUNC.loads(f.read(), **kwargs)
    
    def read_jsonl(self, mode: str = 'rb+', use_parser: bool = False, skip_errors: bool = True, **kwargs) -> Iterator[T]:
        with self.open(mode=mode) as f:
            for line in f:
                try:
                    l = JSON_PARSER.parse(line, kwargs.get('recursive', False)) if use_parser else JSON_FUNC.loads(line)
                    yield l
                except (KeyboardInterrupt, GeneratorExit): break
                except ValueError:
                    if skip_errors: continue
                    raise e
                except Exception as e:
                    if skip_errors: continue
                    raise e

    def format(self: T, *args: Any, **kwargs: Any) -> T:
        """Apply `str.format()` to the path."""
        return type(self)(os.fspath(self).format(*args, **kwargs))    # pytype: disable=not-instantiable


class ReadWritePath(ReadOnlyPath, Protocol):
    """Protocol for pathlib.Path-like API.
    See [pathlib.Path](https://docs.python.org/3/library/pathlib.html)
    documentation.
    """

    @abc.abstractmethod
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """Create a new directory at this given path."""

    @abc.abstractmethod
    def rmdir(self) -> None:
        """Remove the empty directory at this given path."""

    @abc.abstractmethod
    def rmtree(self) -> None:
        """Remove the directory, including all sub-files."""

    @abc.abstractmethod
    def unlink(self, missing_ok: bool = False) -> None:
        """Remove this file or symbolic link."""
    
    def ensure_dir(self: T, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """Ensures the parent directory exists, creates if not"""
        return self.absolute_parent.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def write_bytes(self, data: bytes) -> None:
        """Writes content as bytes."""
        with self.open('wb') as f:
            return f.write(data)

    def write_text(self, data: str, mode: str = 'auto', encoding: Optional[str] = None, errors: Optional[str] = None) -> None:
        """Writes content as str."""
        if mode == 'auto': mode = 'a' if self.exists() else 'w'
        with self.open(mode) as f:
            if 'a' in mode:
                f.write('\n')
            return f.write(data)
    
    def write_json(self, data: Json, ensure_ascii: bool = False, indent: int = 2, **kwargs) -> None:
        """Writes content as str."""
        with self.open('w') as f:
            return JSON_FUNC.dump(data, f, ensure_ascii=ensure_ascii, indent=indent, **kwargs)
    
    def write_jsonl(self, data: List[Json], mode: str = 'auto', newline: str = '\n', ignore_errors: bool = True, **kwargs) -> None:
        """Writes content as str."""
        if mode == 'auto': mode = 'a' if self.exists() else 'w'
        with self.open(mode=mode) as f:
            for item in data:
                try:
                    f.write(JSON_FUNC.dumps(item, **kwargs))
                    f.write(newline)
                except (KeyboardInterrupt, GeneratorExit): break
                except ValueError:
                    if ignore_errors: continue
                    raise e
                except Exception as e:
                    if ignore_errors: continue
                    raise e

    def write_pkl(self, obj: Any, **kwargs) -> None:
        data = PICKLE_FUNC.dumps(obj, **kwargs)
        return self.write_bytes(data)
    
    def write_pt(self, obj: Any, mode: str = 'wb', **kwargs) -> None:
        with self.open(mode=mode) as f:
            return PT_FUNC.save(obj, f, **kwargs)
    
    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """Create a file at this given path."""
        del mode    # Unused
        if self.exists():
            if exist_ok: return
            else: raise FileExistsError(f'{self} already exists.')
        self.write_text('', mode='w')

    @abc.abstractmethod
    def rename(self: T, target: PathLike) -> T:
        """Renames the path."""

    @abc.abstractmethod
    def replace(self: T, target: PathLike) -> T:
        """Overwrites the destination path."""

    @abc.abstractmethod
    def copy(self: T, dst: PathLike, overwrite: bool = False) -> T:
        """Copy the current file to the given destination."""
    

    def copydir(self: T, dst: PathLike, ignore=['.git'], overwrite: bool = False, dryrun: bool = False) -> List[T]:
        """Copies the Current Top Level Parent Dir to the Dst Dir without recursion"""
        dst = self._new(dst)
        assert dst.is_dir(), 'Destination is not a valid directory'
        if not dryrun: dst.ensure_dir()
        copied_files = []
        fnames = self.listdir(ignore=ignore)
        curdir = self.absolute_parent
        for fname in fnames:
            dest_path = dst.joinpath(fname.relative_to(curdir))
            if not dryrun:
                fname.copy(dest_path, overwrite=overwrite, skip_errors=True)
            copied_files.append(dest_path)
        return copied_files


    def copydirs(self: T, dst: PathLike, mode: str = 'shallow', pattern='*', ignore=['.git'], overwrite: bool = False, levels: int = 2, dryrun: bool = False) -> List[T]:
        """Copies the Current Parent Dir to the Dst Dir.
        modes = [shallow for top level recursive. recursive for all nested]
        levels = number of recursive levels
        dryrun = returns all files that would have been copied without copying
        """
        assert mode in {'shallow', 'recursive'}, 'Invalid Mode Option: [shallow, recursive]'
        dst = self._new(dst)
        assert dst.is_dir(), 'Destination is not a valid directory'
        levels = max(1, levels)
        dst.ensure_dir()
        curdir = self.absolute_parent
        copied_files = []
        if levels > 1 and mode == 'recursive' and '/' not in pattern:
            for _ in range(1, levels): pattern += '/*'
        if self.is_dir() and not pattern.startswith('/'): pattern = '*/' + pattern
        fiter = curdir.glob(pattern) if mode == 'shallow' else curdir.rglob(pattern)
        fnames = [f for f in fiter if not bool(set(f.parts).intersection(ignore))]
        #print(len(fnames))
        for f in fnames:
            dest_path = dst.joinpath(f.relative_to(curdir))
            if not dryrun:
                if f.is_dir():
                    dest_path.ensure_dir()
                else:
                    f.copy(dest_path, overwrite=overwrite, skip_errors=True)
            copied_files.append(dest_path)
        return copied_files
    
    @property
    def absolute_parent(self: T) -> T:
        uri_scheme = self._uri_scheme
        if uri_scheme:
            return self._new(self._PATH.join(f'{uri_scheme}://', '/'.join(self.parts[2:-1])))
        p = self.resolve()
        if p.is_dir: 
            return p
        return p.parent