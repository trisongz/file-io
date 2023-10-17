import os
import sys
import enum
import anyio
import asyncio
import functools
import subprocess

from concurrent import futures

from typing import Callable, Coroutine, Any, Union, List, Awaitable, Optional, TypeVar, AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from anyio._core._eventloop import threadlocals
from .helpers import is_coro_func

if sys.version_info < (3, 10):
    # Add aiter and anext to asyncio
    def aiter(it: AsyncIterable) -> Any:
        return it.__aiter__()
    
    def anext(it: AsyncIterator) -> Any:
        return it.__anext__()


class ThreadPooler:
    pool: futures.ThreadPoolExecutor = None

    @classmethod
    def is_coro(cls, obj: Any) -> bool:
        return is_coro_func(obj)

    @classmethod
    def get_pool(cls) -> futures.ThreadPoolExecutor:
        if cls.pool is None:
            from fileio.utils.configs import get_fileio_settings
            cls.pool = futures.ThreadPoolExecutor(max_workers = get_fileio_settings().num_workers)
        return cls.pool


    @classmethod
    async def run_async(cls, func: Callable, *args, **kwargs):
        """
        Runs a Sync Function as an Async Function
        """
        blocking = functools.partial(func, *args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(cls.get_pool(), blocking)
    
    
    @classmethod
    def run_sync(cls, func: Coroutine, *args, **kwargs):
        """
        Runs an Async Function as a Sync Function
        """
        current_async_module = getattr(threadlocals, "current_async_module", None)
        partial_f = functools.partial(func, *args, **kwargs)
        if current_async_module is None:
            return anyio.run(partial_f)
        return anyio.from_thread.run(partial_f)

    @classmethod
    def run_command(
        cls, 
        command: Union[List[str], str], 
        shell: bool = True, 
        raise_error: bool = True, 
        **kwargs
    ):
        if isinstance(command, list): command = " ".join(command)
        try:
            out = subprocess.check_output(command, shell=shell, **kwargs)
            if isinstance(out, bytes): out = out.decode('utf8')
            return out.strip()
        except Exception as e:
            if not raise_error: return ""
            raise e

    @classmethod
    async def async_run_command(
        cls, 
        command: Union[str, List[str]], 
        output_only: bool = True, 
        stdout = asyncio.subprocess.PIPE, 
        stderr = asyncio.subprocess.PIPE, 
        output_encoding: str = 'UTF-8', 
        output_errors: str = 'ignore', 
        *args,
        **kwargs
    ) -> Union[str, asyncio.subprocess.Process]:
        """
        Executes a Shell command using `asyncio.subprocess.create_subprocess_shell`

        Returns str if output_only else `asyncio.subprocess.Process`
        """
        if isinstance(command, list): command = ' '.join(command)
        p = await asyncio.subprocess.create_subprocess_shell(command, *args, stdout = stdout, stderr = stderr, **kwargs)
        if not output_only: return p
        stdout, _ = await p.communicate()
        return stdout.decode(encoding = output_encoding, errors = output_errors).strip()


    @classmethod
    async def asyncish(cls, func: Callable, *args, **kwargs):
        """
        Runs a Function as an Async Function if it is an Async Function
        otherwise wraps it around `run_async`
        """
        if cls.is_coro(func): return await func(*args, **kwargs)
        return await cls.run_async(func, *args, **kwargs)



def ensure_coro(
    func: Callable[..., Any]
) -> Callable[..., Awaitable[Any]]:
    """
    Ensure that the function is a coroutine
    """
    if asyncio.iscoroutinefunction(func): return func
    @functools.wraps(func)
    async def inner(*args, **kwargs):
        return await ThreadPooler.asyncish(func, *args, **kwargs)
    return inner

class ReturnWhenType(str, enum.Enum):
    """
    Return When Type
    """
    FIRST_COMPLETED = "FIRST_COMPLETED"
    FIRST_EXCEPTION = "FIRST_EXCEPTION"
    ALL_COMPLETED = "ALL_COMPLETED"

    @property
    def val(self) -> Union[asyncio.FIRST_COMPLETED, asyncio.FIRST_EXCEPTION, asyncio.ALL_COMPLETED]:
        """
        Get the value of the return when type
        """
        return getattr(asyncio, self.value)


_concurrency_limit: Optional[int] = None

def set_concurrency_limit(
    limit: Optional[int] = None
):
    """
    Set the concurrency limit
    """
    global _concurrency_limit
    if limit is None: limit = os.cpu_count() * 4
    _concurrency_limit = limit

def get_concurrency_limit() -> Optional[int]:
    """
    Get the concurrency limit
    """
    if _concurrency_limit is None: set_concurrency_limit()
    return _concurrency_limit


async def limit_concurrency(
    mapped_iterable: Union[Callable[[], Awaitable[Any]], Awaitable[Any], Coroutine[Any, Any, Any], Callable[[], Any]],
    limit: Optional[int] = None,
    return_when: Optional[ReturnWhenType] = ReturnWhenType.FIRST_COMPLETED,
):
    """
    Limit the concurrency of an iterable

    Args:
        mapped_iterable (Union[Callable[[], Awaitable[Any]], Awaitable[Any], Coroutine[Any, Any, Any], Callable[[], Any]]): The iterable to limit the concurrency of
        limit (Optional[int], optional): The limit of the concurrency. Defaults to None.
        return_when (Optional[ReturnWhenType], optional): The return when type. Defaults to ReturnWhenType.FIRST_COMPLETED.
    
    Yields:
        [type]: [description]
    """
    try:
        iterable = aiter(mapped_iterable)
        is_async = True
    except (TypeError, AttributeError):
        iterable = iter(mapped_iterable)
        is_async = False
    
    iterable_ended: bool = False
    pending = set()
    limit = get_concurrency_limit() if limit is None else limit
    return_when = ReturnWhenType(return_when) if isinstance(return_when, str) else return_when

    while pending or not iterable_ended:
        while len(pending) < limit and not iterable_ended:
            try:
                iter_item = await anext(iterable) if is_async else next(iterable)
            except StopAsyncIteration if is_async else StopIteration:
                iterable_ended = True
            else:
                pending.add(asyncio.ensure_future(iter_item))

        if not pending: return
        done, pending = await asyncio.wait(
            pending, 
            return_when = return_when.val
        )
        while done: yield done.pop()

RT = TypeVar("RT")

async def async_map(
    func: Callable[..., Awaitable[Any]],
    iterable: Iterable[Any], 
    *args,
    limit: Optional[int] = None,
    return_when: Optional[ReturnWhenType] = ReturnWhenType.FIRST_COMPLETED,
    **kwargs,
) -> AsyncGenerator[RT, None]:
    """
    Async Map of a function with args and kwargs

    Args:
        func (Callable[..., Awaitable[Any]]): The function to map
        iterable (Iterable[Any]): The iterable to map
        limit (Optional[int], optional): The limit of the concurrency. Defaults to None.
        return_when (Optional[ReturnWhenType], optional): The return when type. Defaults to ReturnWhenType.FIRST_COMPLETED.
    
    Yields:
        [type]: [description]
    """
    func = ensure_coro(func)
    partial = functools.partial(func, *args, **kwargs)
    try:
        mapped_iterable = map(partial, iterable)
    except TypeError:
        mapped_iterable = (partial(x) async for x in iterable)
    async for task in limit_concurrency(mapped_iterable, limit = limit, return_when = return_when):
        yield await task