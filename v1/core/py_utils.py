import os
import io
import sys
import base64
import contextlib
import functools
import uuid
import itertools
import logging
import random
import shutil
import string
import textwrap
import threading
import typing
import inspect
from six.moves import urllib
from typing import Any, Callable, Iterator, List, NoReturn, Optional, Tuple, Type, TypeVar, Union

from fileio.core import constants
from fileio.core import file_adapters
from fileio.core import type_utils
from fileio.core.libs import TF_FUNC

Tree = type_utils.Tree

# NOTE: When used on an instance method, the cache is shared across all
# instances and IS NOT per-instance.
# See
# https://stackoverflow.com/questions/14946264/python-lru-cache-decorator-per-instance
# For @property methods, use @memoized_property below.
memoize = functools.lru_cache

T = TypeVar('T')

Fn = TypeVar('Fn', bound=Callable[..., Any])


def is_notebook():
    """Returns True if running in a notebook (Colab, Jupyter) environment."""
    # Inspired from the tqdm autonotebook code
    try:
        # Use sys.module as we do not want to trigger import
        IPython = sys.modules['IPython']    # pylint: disable=invalid-name
        if 'IPKernelApp' not in IPython.get_ipython().config:
            return False    # Run in a IPython terminal
    except:    # pylint: disable=bare-except
        return False
    else:
        return True


@contextlib.contextmanager
def temporary_assignment(obj, attr, value):
    """Temporarily assign obj.attr to value."""
    original = getattr(obj, attr)
    setattr(obj, attr, value)
    try:
        yield
    finally:
        setattr(obj, attr, original)


def zip_dict(*dicts):
    """Iterate over items of dictionaries grouped by their keys."""
    for key in set(itertools.chain(*dicts)):    # set merge all keys
        # Will raise KeyError if the dict don't have the same keys
        yield key, tuple(d[key] for d in dicts)


@contextlib.contextmanager
def disable_logging():
    """Temporarily disable the logging."""
    logger = logging.getLogger()
    logger_disabled = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = logger_disabled


class NonMutableDict(dict):
    """Dict where keys can only be added but not modified.
    Will raise an error if the user try to overwrite one key. The error message
    can be customized during construction. It will be formatted using {key} for
    the overwritten key.
    """

    def __init__(self, *args, **kwargs):
        self._error_msg = kwargs.pop(
                'error_msg',
                'Try to overwrite existing key: {key}',
        )
        if kwargs:
            raise ValueError('NonMutableDict cannot be initialized with kwargs.')
        super(NonMutableDict, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if key in self:
            raise ValueError(self._error_msg.format(key=key))
        return super(NonMutableDict, self).__setitem__(key, value)

    def update(self, other):
        if any(k in self for k in other):
            raise ValueError(self._error_msg.format(key=set(self) & set(other)))
        return super(NonMutableDict, self).update(other)


class classproperty(property):
    """Descriptor to be used as decorator for @classmethods."""

    def __get__(self, obj, objtype=None):
        return self.fget.__get__(None, objtype)()


class memoized_property(property):
    """Descriptor that mimics @property but caches output in member variable."""

    def __get__(self, obj, objtype=None):
        # See https://docs.python.org/3/howto/descriptor.html#properties
        if obj is None:
            return self
        if self.fget is None:    # pytype: disable=attribute-error
            raise AttributeError('unreadable attribute')
        attr = '__cached_' + self.fget.__name__    # pytype: disable=attribute-error
        cached = getattr(obj, attr, None)
        if cached is None:
            cached = self.fget(obj)
            setattr(obj, attr, cached)
        return cached


if typing.TYPE_CHECKING:
    # TODO(b/171883689): There is likelly better way to annotate descriptors

    def classproperty(fn: Callable[[Type[Any]], T]) -> T:    # pylint: disable=function-redefined
        return fn(type(None))

    def memoized_property(fn: Callable[[Any], T]) -> T:    # pylint: disable=function-redefined
        return fn(None)


def map_nested(function, data_struct, dict_only=False, map_tuple=False):
    """Apply a function recursively to each element of a nested data struct."""

    # Could add support for more exotic data_struct, like OrderedDict
    if isinstance(data_struct, dict):
        return {
                k: map_nested(function, v, dict_only, map_tuple)
                for k, v in data_struct.items()
        }
    elif not dict_only:
        types_ = [list]
        if map_tuple:
            types_.append(tuple)
        if isinstance(data_struct, tuple(types_)):
            mapped = [
                    map_nested(function, v, dict_only, map_tuple) for v in data_struct
            ]
            if isinstance(data_struct, list):
                return mapped
            else:
                return tuple(mapped)
    # Singleton
    return function(data_struct)


def zip_nested(arg0, *args, **kwargs):
    """Zip data struct together and return a data struct with the same shape."""
    # Python 2 do not support kwargs only arguments
    dict_only = kwargs.pop('dict_only', False)
    assert not kwargs

    # Could add support for more exotic data_struct, like OrderedDict
    if isinstance(arg0, dict):
        return {
                k: zip_nested(*a, dict_only=dict_only)
                for k, a in zip_dict(arg0, *args)
        }
    elif not dict_only:
        if isinstance(arg0, list):
            return [zip_nested(*a, dict_only=dict_only) for a in zip(arg0, *args)]
    # Singleton
    return (arg0,) + args


def flatten_nest_dict(d):
    """Return the dict with all nested keys flattened joined with '/'."""
    # Use NonMutableDict to ensure there is no collision between features keys
    flat_dict = NonMutableDict()
    for k, v in d.items():
        if isinstance(v, dict):
            flat_dict.update({
                    '{}/{}'.format(k, k2): v2 for k2, v2 in flatten_nest_dict(v).items()
            })
        else:
            flat_dict[k] = v
    return flat_dict


# Note: Could use `tree.flatten_with_path` instead, but makes it harder for
# users to compile from source.
def flatten_with_path(
        structure: Tree[T],
) -> Iterator[Tuple[Tuple[Union[str, int], ...], T]]:
    """Convert a TreeDict into a flat list of paths and their values.
    ```py
    flatten_with_path({'a': {'b': v}}) == [(('a', 'b'), v)]
    ```
    Args:
        structure: Nested input structure
    Yields:
        The `(path, value)` tuple. With path being the tuple of `dict` keys and
            `list` indexes
    """
    if isinstance(structure, dict):
        key_struct_generator = sorted(structure.items())
    elif isinstance(structure, (list, tuple)):
        key_struct_generator = enumerate(structure)
    else:
        key_struct_generator = None    # End of recursion

    if key_struct_generator is not None:
        for key, sub_structure in key_struct_generator:
            # Recurse into sub-structures
            for sub_path, sub_value in flatten_with_path(sub_structure):
                yield (key,) + sub_path, sub_value
    else:
        yield (), structure    # Leaf, yield value


def dedent(text):
    """Wrapper around `textwrap.dedent` which also `strip()` and handle `None`."""
    return textwrap.dedent(text).strip() if text else text


def indent(text: str, indent: str) -> str:    # pylint: disable=redefined-outer-name
    text = dedent(text)
    return text.replace('\n', '\n' + indent)


def pack_as_nest_dict(flat_d, nest_d):
    """Pack a 1-lvl dict into a nested dict with same structure as `nest_d`."""
    nest_out_d = {}
    for k, v in nest_d.items():
        if isinstance(v, dict):
            v_flat = flatten_nest_dict(v)
            sub_d = {
                    k2: flat_d.pop('{}/{}'.format(k, k2)) for k2, _ in v_flat.items()
            }
            # Recursivelly pack the dictionary
            nest_out_d[k] = pack_as_nest_dict(sub_d, v)
        else:
            nest_out_d[k] = flat_d.pop(k)
    if flat_d:    # At the end, flat_d should be empty
        raise ValueError(
                'Flat dict strucure do not match the nested dict. Extra keys: '
                '{}'.format(list(flat_d.keys())))
    return nest_out_d


@contextlib.contextmanager
def nullcontext(enter_result: T = None) -> Iterator[T]:
    """Backport of `contextlib.nullcontext`."""
    yield enter_result


def _get_incomplete_path(filename):
    """Returns a temporary filename based on filename."""
    random_suffix = ''.join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    return filename + '.incomplete' + random_suffix


@contextlib.contextmanager
def incomplete_dir(dirname: type_utils.PathLike) -> Iterator[str]:
    """Create temporary dir for dirname and rename on exit."""
    dirname = os.fspath(dirname)
    tmp_dir = _get_incomplete_path(dirname)
    TF_FUNC.io.gfile.makedirs(tmp_dir)
    try:
        yield tmp_dir
        TF_FUNC.io.gfile.rename(tmp_dir, dirname)
    finally:
        if TF_FUNC.io.gfile.exists(tmp_dir):
            TF_FUNC.io.gfile.rmtree(tmp_dir)


@contextlib.contextmanager
def incomplete_file(
        path: type_utils.ReadWritePath,) -> Iterator[type_utils.ReadWritePath]:
    """Writes to path atomically, by writing to temp file and renaming it."""
    tmp_path = path.parent / f'{path.name}.incomplete.{uuid.uuid4().hex}'
    try:
        yield tmp_path
        tmp_path.replace(path)
    finally:
        # Eventually delete the tmp_path if exception was raised
        tmp_path.unlink(missing_ok=True)


@contextlib.contextmanager
def atomic_write(path, mode):
    """Writes to path atomically, by writing to temp file and renaming it."""
    tmp_path = '%s%s_%s' % (path, constants.INCOMPLETE_SUFFIX, uuid.uuid4().hex)
    with TF_FUNC.io.gfile.GFile(tmp_path, mode) as file_:
        yield file_
    TF_FUNC.io.gfile.rename(tmp_path, path, overwrite=True)


def reraise(
        e: Exception,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
) -> NoReturn:
    """Reraise an exception with an additional message."""
    prefix = prefix or ''
    suffix = '\n' + suffix if suffix else ''

    # If unsure about modifying the function inplace, create a new exception
    # and stack it in the chain.
    if (
            # Exceptions with custom error message
            type(e).__str__ is not BaseException.__str__
            # This should never happens unless the user plays with Exception
            # internals
            or not hasattr(e, 'args') or not isinstance(e.args, tuple)):
        msg = f'{prefix}{e}{suffix}'
        # Could try to dynamically create a
        # `type(type(e).__name__, (ReraisedError, type(e)), {})`, but should be
        # carefull when nesting `reraise` as well as compatibility with external
        # code.
        # Some base exception class (ImportError, OSError) and subclasses (
        # ModuleNotFoundError, FileNotFoundError) have custom `__str__` error
        # message. We re-raise those with same type to allow except in caller code.
        if isinstance(e, (ImportError, OSError)):
            exception = type(e)(msg)
        else:
            exception = RuntimeError(f'{type(e).__name__}: {msg}')
        raise exception from e
    # Otherwise, modify the exception in-place
    elif len(e.args) <= 1:
        exception_msg = e.args[0] if e.args else ''
        e.args = (f'{prefix}{exception_msg}{suffix}',)
        raise    # pylint: disable=misplaced-bare-raise
    # If there is more than 1 args, concatenate the message with other args
    else:
        e.args = tuple(
                p for p in (prefix,) + e.args + (suffix,)
                if not isinstance(p, str) or p)
        raise    # pylint: disable=misplaced-bare-raise


@contextlib.contextmanager
def try_reraise(*args, **kwargs):
    """Context manager which reraise exceptions with an additional message.
    Contrary to `raise ... from ...` and `raise Exception().with_traceback(tb)`,
    this function tries to modify the original exception, to avoid nested
    `During handling of the above exception, another exception occurred:`
    stacktraces.
    Args:
        *args: Prefix to add to the exception message
        **kwargs: Prefix to add to the exception message
    Yields:
        None
    """
    try:
        yield
    except Exception as e:    # pylint: disable=broad-except
        reraise(e, *args, **kwargs)


def rgetattr(obj, attr, *args):
    """Get attr that handles dots in attr name."""

    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split('.'))


def has_sufficient_disk_space(needed_bytes, directory='.'):
    try:
        free_bytes = shutil.disk_usage(os.path.abspath(directory)).free
    except OSError:
        return True
    return needed_bytes < free_bytes


def get_class_path(cls, use_tfds_prefix=True):
    """Returns path of given class or object. Eg: `tfds.image.cifar.Cifar10`."""
    if not isinstance(cls, type):
        cls = cls.__class__
    module_path = cls.__module__
    if use_tfds_prefix and module_path.startswith('tensorflow_datasets'):
        module_path = 'tfds' + module_path[len('tensorflow_datasets'):]
    return '.'.join([module_path, cls.__name__])


def get_class_url(cls):
    """Returns URL of given class or object."""
    cls_path = get_class_path(cls, use_tfds_prefix=False)
    module_path, unused_class_name = cls_path.rsplit('.', 1)
    module_path = module_path.replace('.', '/')
    return constants.SRC_BASE_URL + module_path + '.py'


def build_synchronize_decorator() -> Callable[[Fn], Fn]:
    """Returns a decorator which prevents concurrent calls to functions.
    Usage:
        synchronized = build_synchronize_decorator()
        @synchronized
        def read_value():
            ...
        @synchronized
        def write_value(x):
            ...
    Returns:
        make_threadsafe (fct): The decorator which lock all functions to which it
            is applied under a same lock
    """
    lock = threading.Lock()

    def lock_decorator(fn: Fn) -> Fn:

        @functools.wraps(fn)
        def lock_decorated(*args, **kwargs):
            with lock:
                return fn(*args, **kwargs)

        return lock_decorated

    return lock_decorator


def basename_from_url(url: str) -> str:
    """Returns file name of file at given url."""
    filename = urllib.parse.urlparse(url).path
    filename = os.path.basename(filename)
    # Replace `%2F` (html code for `/`) by `_`.
    # This is consistent with how Chrome rename downloaded files.
    filename = filename.replace('%2F', '_')
    return filename or 'unknown_name'


def list_info_files(dir_path: type_utils.PathLike) -> List[str]:
    """Returns name of info files within dir_path."""
    path = os.fspath(dir_path)
    return [
            fname for fname in TF_FUNC.io.gfile.listdir(path)
            if not TF_FUNC.io.gfile.isdir(os.path.join(path, fname)) and
            not file_adapters.is_example_file(fname)
    ]


def get_base64(write_fn: Union[bytes, Callable[[io.BytesIO], None]],) -> str:
    """Extracts the base64 string of an object by writing into a tmp buffer."""
    if isinstance(write_fn, bytes):    # Value already encoded
        bytes_value = write_fn
    else:
        buffer = io.BytesIO()
        write_fn(buffer)
        bytes_value = buffer.getvalue()
    return base64.b64encode(bytes_value).decode('ascii')    # pytype: disable=bad-return-type


@contextlib.contextmanager
def add_sys_path(path: type_utils.PathLike) -> Iterator[None]:
    """Temporary add given path to `sys.path`."""
    path = os.fspath(path)
    try:
        sys.path.insert(0, path)
        yield
    finally:
        sys.path.remove(path)


WORKAROUND_SCHEMES = ['s3', 's3n', 's3u', 's3a', 'gs']
QUESTION_MARK_PLACEHOLDER = '///fileio.core.py_utils.QUESTION_MARK_PLACEHOLDER///'



def inspect_kwargs(kallable):
    try:
        signature = inspect.signature(kallable)
    except AttributeError:
        try:
            args, varargs, keywords, defaults = inspect.getargspec(kallable)
        except TypeError:
            return {}

        if not defaults:
            return {}
        supported_keywords = args[-len(defaults):]
        return dict(zip(supported_keywords, defaults))
    else:
        return {
            name: param.default
            for name, param in signature.parameters.items()
            if param.default != inspect.Parameter.empty
        }


def check_kwargs(kallable, kwargs):
    """Check which keyword arguments the callable supports.
    Parameters
    ----------
    kallable: callable
        A function or method to test
    kwargs: dict
        The keyword arguments to check.  If the callable doesn't support any
        of these, a warning message will get printed.
    Returns
    -------
    dict
        A dictionary of argument names and values supported by the callable.
    """
    supported_keywords = sorted(inspect_kwargs(kallable))
    #unsupported_keywords = [k for k in sorted(kwargs) if k not in supported_keywords]
    supported_kwargs = {k: v for (k, v) in kwargs.items() if k in supported_keywords}

    return supported_kwargs


def safe_urlsplit(url):
    """This is a hack to prevent the regular urlsplit from splitting around question marks.
    A question mark (?) in a URL typically indicates the start of a
    querystring, and the standard library's urlparse function handles the
    querystring separately.  Unfortunately, question marks can also appear
    _inside_ the actual URL for some schemas like S3, GS.
    Replaces question marks with a special placeholder substring prior to
    splitting.  This work-around behavior is disabled in the unlikely event the
    placeholder is already part of the URL.  If this affects you, consider
    changing the value of QUESTION_MARK_PLACEHOLDER to something more suitable.
    See Also
    --------
    https://bugs.python.org/issue43882
    https://github.com/python/cpython/blob/3.7/Lib/urllib/parse.py
    https://github.com/RaRe-Technologies/smart_open/issues/285
    https://github.com/RaRe-Technologies/smart_open/issues/458
    smart_open/utils.py:QUESTION_MARK_PLACEHOLDER
    """
    sr = urllib.parse.urlsplit(url, allow_fragments=False)

    placeholder = None
    if sr.scheme in WORKAROUND_SCHEMES and '?' in url and QUESTION_MARK_PLACEHOLDER not in url:
        #
        # This is safe because people will _almost never_ use the below
        # substring in a URL.  If they do, then they're asking for trouble,
        # and this special handling will simply not happen for them.
        #
        placeholder = QUESTION_MARK_PLACEHOLDER
        url = url.replace('?', placeholder)
        sr = urllib.parse.urlsplit(url, allow_fragments=False)

    if placeholder is None:
        return sr

    path = sr.path.replace(placeholder, '?')
    return urllib.parse.SplitResult(sr.scheme, sr.netloc, path, '', '')
