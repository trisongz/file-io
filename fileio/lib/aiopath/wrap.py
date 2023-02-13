import asyncio

from concurrent import futures
from functools import wraps, partial
from typing import Callable, Any, Awaitable
from fileio.utils.configs import settings

CoroutineResult = Awaitable[Any]
CoroutineFunction = Callable[..., CoroutineResult]
CoroutineMethod = Callable[..., CoroutineResult]

_pool = futures.ThreadPoolExecutor(max_workers = settings.core.num_workers)

async def to_thread(func: Callable, *args, **kwargs) -> Any:
    # anyio's run_sync() doesn't support passing kwargs
    func_kwargs = partial(func, *args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_pool, func_kwargs)

def func_to_async_func(func: Callable) -> CoroutineFunction:
    @wraps(func)
    async def new_func(*args, **kwargs) -> Any:
        return await to_thread(func, *args, **kwargs)

    return new_func


method_as_method_coro = func_to_async_func


def func_as_method_coro(func: Callable) -> CoroutineMethod:
    @wraps(func)
    async def method(self, *args, **kwargs) -> Any:
        return await to_thread(func, *args, **kwargs)
    return method


def coro_as_method_coro(coro: CoroutineFunction) -> CoroutineMethod:
    @wraps(coro)
    async def method(self, *args, **kwargs) -> Any:
        return await coro(*args, **kwargs)
    return method