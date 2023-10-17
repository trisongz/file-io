# handle posix stat stuff
import os
import sys
import time
import inspect
import random
import typing
import asyncio
import hashlib
import functools
import contextlib
import datetime
from email.utils import formatdate
from mimetypes import guess_type as mimetypes_guess_type

from frozendict import frozendict
from fileio.utils.logs import default_logger


try:
    # check if the Python version supports the parameter
    # using usedforsecurity=False to avoid an exception on FIPS systems
    # that reject usedforsecurity=True
    hashlib.md5(b"data", usedforsecurity=False)  # type: ignore[call-arg]

    def md5_hexdigest(
        data: bytes, *, usedforsecurity: bool = True
    ) -> str:  # pragma: no cover
        return hashlib.md5(  # type: ignore[call-arg]
            data, usedforsecurity=usedforsecurity
        ).hexdigest()

except TypeError:  # pragma: no cover

    def md5_hexdigest(data: bytes, *, usedforsecurity: bool = True) -> str:
        return hashlib.md5(data).hexdigest()

# Compatibility wrapper for `mimetypes.guess_type` to support `os.PathLike` on <py3.8
def guess_type(
    path: typing.Union[str, "os.PathLike[str]"], strict: bool = True
) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    if sys.version_info < (3, 8):  # pragma: no cover
        path = os.fspath(path)
    return mimetypes_guess_type(path, strict)


def get_file_info(path: typing.Union[str, "os.PathLike[str]"]) -> typing.Dict[str, typing.Any]:
    if sys.version_info < (3, 8):  # pragma: no cover
        path = os.fspath(path)
    stat_result = os.stat(path)
    last_modified = formatdate(stat_result.st_mtime, usegmt=True)
    etag_base = f"{str(stat_result.st_mtime)}-{str(stat_result.st_size)}"
    etag = md5_hexdigest(etag_base.encode(), usedforsecurity=False)
    return {
        "size": stat_result.st_size,
        "last-modified": last_modified,
        "etag": etag,
        "media-type": guess_type(path)[0] or "text/plain",
        "filename": os.path.basename(path),
    }

def timer(t: typing.Optional[float] = None, msg: typing.Optional[str] = None, logger = default_logger):
    if not t: return time.perf_counter()
    done_time = time.perf_counter() - t
    if msg: logger.info(f'{msg} in {done_time:.2f} secs')
    return done_time

def timed(func: typing.Callable):
    """
    Decorator to time a function
    """
    _func_name = func.__name__
    @functools.wraps(func)
    async def fx(*args, **kwargs):
        start = time.perf_counter()
        if inspect.iscoroutinefunction(func): result = await func(*args, **kwargs)
        else: result = func(*args, **kwargs)
        end = time.perf_counter()
        default_logger.info(f'{_func_name}: {end - start:.4f} secs')
        return result
    return fx


def exponential_backoff(
    attempts: int,
    base_delay: int = 1,
    max_delay: int = None,
    jitter: bool = True,
):
    """
    Get the next delay for retries in exponential backoff.

    attempts: Number of attempts so far
    base_delay: Base delay, in seconds
    max_delay: Max delay, in seconds. If None (default), there is no max.
    jitter: If True, add a random jitter to the delay
    """
    if max_delay is None:
        max_delay = float("inf")
    backoff = min(max_delay, base_delay * 2 ** max(attempts - 1, 0))
    if jitter:
        backoff = backoff * random.random()
    return backoff

def retryable(limit: int = 3, delay: int = 3):
    def decorator(func: typing.Callable):
        if not inspect.iscoroutinefunction(func):
            def sync_wrapper(*args, **kwargs):
                for n in range(limit - 1):
                    with contextlib.suppress(Exception):
                        return func(*args, **kwargs)
                    time.sleep(exponential_backoff(n, base_delay=delay))
                return func(*args, **kwargs)
            return sync_wrapper
        else:
            async def async_wrapper(*args, **kwargs):
                for n in range(limit-1):
                    with contextlib.suppress(Exception):
                        return await func(*args, **kwargs)
                    await asyncio.sleep(exponential_backoff(n, base_delay=delay))
                return await func(*args, **kwargs)
            return async_wrapper
    return decorator


def create_timestamp(
    tz: typing.Optional[datetime.tzinfo] = datetime.timezone.utc,
    as_str: typing.Optional[bool] = False,
):
    """
    Creates a timestamp
    args:
        tz: timezone
        as_str: if True, returns a string
    """
    dt = datetime.datetime.now(tz =tz)
    return dt.isoformat() if as_str else dt

def is_coro_func(obj, func_name: str = None):
    """
    This is probably in the library elsewhere but returns bool
    based on if the function is a coro
    """
    try:
        if inspect.iscoroutinefunction(obj): return True
        if inspect.isawaitable(obj): return True
        if func_name and hasattr(obj, func_name) and inspect.iscoroutinefunction(getattr(obj, func_name)):
            return True
        return bool(hasattr(obj, '__call__') and inspect.iscoroutinefunction(obj.__call__))

    except Exception:
        return False


"""
Timed Caches
"""

def recursive_freeze(value):
    if not isinstance(value, dict):
        return value
    for k,v in value.items():
        value[k] = recursive_freeze(v)
    return frozendict(value)

# To unfreeze
def recursive_unfreeze(value):
    if isinstance(value, frozendict):
        value = dict(value)
        for k,v in value.items():
            value[k] = recursive_unfreeze(v)
    
    return value

def freeze_args_and_kwargs(*args, **kwargs):
    args = tuple(
        recursive_freeze(arg) if isinstance(arg, dict) else arg
        for arg in args
    )
    kwargs = {k: recursive_freeze(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
    return args, kwargs



def timed_cache(
    secs: typing.Optional[int] = 60 * 60, 
    maxsize: int = 1024
):
    """
    Wrapper for creating a expiring cached function
    args:
        secs: number of seconds to cache the function
        maxsize: maxsize of the cache
    """
    if secs is None: secs = 60 * 60
    def wrapper_cache(func):
        func = functools.lru_cache(maxsize=maxsize)(func)
        func.lifetime = datetime.timedelta(seconds=secs)
        func.expiration = create_timestamp() + func.lifetime
        def _check_cache(func):
            if create_timestamp() >= func.expiration:
                func.cache_clear()
                func.expiration = create_timestamp() + func.lifetime

        if is_coro_func(func):
            # https://stackoverflow.com/questions/34116942/how-to-cache-asyncio-coroutines
            @functools.wraps(func)
            def wrapped_func(*args, **kwargs):
                _check_cache(func)
                args, kwargs = freeze_args_and_kwargs(*args, **kwargs)
                coro = func(*args, **kwargs)
                return asyncio.ensure_future(coro)
                # return await func(*args, **kwargs)
            return wrapped_func

        else:
            @functools.wraps(func)
            def wrapped_func(*args, **kwargs):
                _check_cache(func)
                args, kwargs = freeze_args_and_kwargs(*args, **kwargs)
                return func(*args, **kwargs)
            return wrapped_func

    return wrapper_cache


__all__ = [
    "get_file_info",
    "md5_hexdigest",
    "mimetypes_guess_type",
    "guess_type",
    'timer',
    'timed',
    'exponential_backoff',
    'retryable',
    'create_timestamp',

]