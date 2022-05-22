import abc


class BaseCloudFile(abc.ABC):
    client = None

    @abc.abstractmethod
    def open(self, filepath, *args, **kwargs):
        raise NotImplementedError
    
    @abc.abstractmethod
    async def aopen(self, filepath, *args, **kwargs) -> None:
        raise NotImplementedError
    
    @abc.abstractmethod
    def close(self):
        raise NotImplementedError
    
    @abc.abstractmethod
    async def aclose(self) -> None:
        raise NotImplementedError
    
    @abc.abstractmethod
    def terminate(self):
        raise NotImplementedError

    def __enter__(self):
        return self

    async def __aenter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.terminate()
        else:
            self.close()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
        

#class BaseCloudClient(object):
