from __future__ import annotations

"""
Contains the base metaclass for providers
"""
import atexit
import asyncio
import contextlib
from types import ModuleType
from typing import Callable, Any, Optional, Coroutine, Type, Union, List, ClassVar, TYPE_CHECKING

from fileio.lib.core import NormalAccessor
from fileio.lib.posix.base import rewrite_async_syntax, func_as_method_coro
# from fileio.utils import LazyLib, settings, logger
from fileio.utils import LazyLib, get_fileio_settings, logger

if TYPE_CHECKING:
    from fileio.providers.tfio import tfFS
    from fsspec.asyn import AsyncFileSystem
    from fileio.utils.configs import FileIOSettings

    with contextlib.suppress(ImportError):
        from s3transfer.manager import TransferManager

class CloudFileSystemType(type):
    fs: ModuleType = None
    fsa: ModuleType = None
    fs_name: str = None # gcsfs
    boto: ModuleType = None
    s3t: 'TransferManager' = None
    tffs: 'tfFS' = None

    #s3t: 'boto3.s3.transfer.TransferManager' = None
    _settings: Optional['FileIOSettings'] = None

    @property
    def settings(cls) -> 'FileIOSettings':
        if cls._settings is None:
            cls._settings = get_fileio_settings()
        return cls._settings

    def is_ready(cls):
        return bool(cls.fsa and cls.fs)
    
    # @classmethod
    def build_gcsfs(cls, **auth_config):
        LazyLib.import_lib('gcsfs')
        import gcsfs
        if auth_config: cls.settings.gcp.update_auth(**auth_config)
        config = cls.settings.gcp.build_gcsfs_config()
        cls.fs = gcsfs.GCSFileSystem(asynchronous = False, **config)
        cls.fsa = rewrite_async_syntax(gcsfs.GCSFileSystem(asynchronous=True, **config), 'gs')
        
        if cls.settings.tfio_enabled:
            from fileio.providers.tfio import tfFS
            if tfFS.enabled:
                logger.info('Leveraging Tensorflow IO support for GCS')
                cls.tffs = tfFS
        
    
    def build_s3fs(cls, **auth_config):
        LazyLib.import_lib('s3fs')
        LazyLib.import_lib('boto3')

        import s3fs
        import boto3
        import boto3.s3.transfer as s3transfer
        from botocore.config import Config as BotoConfig
    
        if auth_config: cls.settings.aws.update_auth(**auth_config)
        config = cls.settings.aws.build_s3fs_config()
        cls.fs = s3fs.S3FileSystem(asynchronous = False, **config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **config))
        
        boto_config = BotoConfig(max_pool_connections = cls.settings.core.num_workers * 2)
        cls.boto = boto3.client('s3', region_name = cls.settings.aws.aws_region, config = boto_config)
        transfer_config = s3transfer.TransferConfig(
            use_threads = True,
            max_concurrency = cls.settings.core.num_workers,
        )
        def create_s3t():
            return s3transfer.create_transfer_manager(cls.boto, transfer_config)

        # @property
        # def get_s3t(self):
        #     return create_s3t()
        
        cls.s3t = property(create_s3t)

    
    def build_minio(cls, **auth_config):
        LazyLib.import_lib('s3fs')
        LazyLib.import_lib('boto3')
        
        import s3fs
        import boto3
        import boto3.s3.transfer as s3transfer
        from botocore.config import Config as BotoConfig

        if auth_config: cls.settings.minio.update_auth(**auth_config)
        config = cls.settings.minio.build_s3fs_config()

        cls.fs = s3fs.S3FileSystem(asynchronous=False, **config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **config))
                
        boto_config = BotoConfig(
            signature_version = cls.settings.minio.minio_signature_ver,
            max_pool_connections = cls.settings.core.num_workers * 2
        )
        cls.boto = boto3.client(
            's3', 
            region_name = cls.settings.minio.minio_region, 
            endpoint_url = cls.settings.minio.minio_endpoint, 
            aws_access_key_id = cls.settings.minio.minio_access_key, 
            aws_secret_access_key = cls.settings.minio.minio_secret_key, 
            config = boto_config
        )
        transfer_config = s3transfer.TransferConfig(
            use_threads = True,
            max_concurrency = cls.settings.core.num_workers
        )
        def create_s3t():
            return s3transfer.create_transfer_manager(cls.boto, transfer_config)
        
        # cls.s3t = create_s3t
        cls.s3t = property(create_s3t)

    def build_s3c(cls, **auth_config):
        LazyLib.import_lib('s3fs')
        LazyLib.import_lib('boto3')
        
        import s3fs
        import boto3
        import boto3.s3.transfer as s3transfer
        from botocore.config import Config as BotoConfig

        if auth_config: cls.settings.s3_compat.update_auth(**auth_config)
        config = cls.settings.s3_compat.build_s3fs_config()

        cls.fs = s3fs.S3FileSystem(asynchronous=False, **config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **config))
                
        boto_config = BotoConfig(
            signature_version = cls.settings.s3_compat.s3_compat_signature_ver,
            max_pool_connections = cls.settings.core.num_workers * 2
        )
        cls.boto = boto3.client(
            's3', 
            region_name = cls.settings.s3_compat.s3_compat_region, 
            endpoint_url = cls.settings.s3_compat.s3_compat_endpoint, 
            aws_access_key_id = cls.settings.s3_compat.s3_compat_access_key, 
            aws_secret_access_key = cls.settings.s3_compat.s3_compat_secret_key, 
            config = boto_config
        )
        transfer_config = s3transfer.TransferConfig(
            use_threads = True,
            max_concurrency = cls.settings.core.num_workers
        )
        def create_s3t():
            return s3transfer.create_transfer_manager(cls.boto, transfer_config)
        
        # cls.s3t = create_s3t
        cls.s3t = property(create_s3t)


    def build_r2(cls, **auth_config):
        LazyLib.import_lib('s3fs')
        LazyLib.import_lib('boto3')
        
        import s3fs
        import boto3
        import boto3.s3.transfer as s3transfer
        from botocore.config import Config as BotoConfig
        from fileio.providers.filesys.cloudflare_r2 import R2FileSystem

        if auth_config: cls.settings.r2.update_auth(**auth_config)
        config = cls.settings.r2.build_s3fs_config()
        cls.fs: R2FileSystem = R2FileSystem(asynchronous=False, **config)
        cls.fsa: R2FileSystem = rewrite_async_syntax(R2FileSystem(asynchronous=True, **config))
        # cls.fs = s3fs.S3FileSystem(asynchronous=False, **config)
        # cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **config))

                
        boto_config = BotoConfig(
            max_pool_connections = cls.settings.core.num_workers * 2
        )
        cls.boto = boto3.client(
            's3', 
            # region_name = "auto", 
            endpoint_url = cls.settings.r2.r2_endpoint, 
            aws_access_key_id = cls.settings.r2.r2_access_key_id, 
            aws_secret_access_key = cls.settings.r2.r2_secret_access_key, 
            config = boto_config
        )
        transfer_config = s3transfer.TransferConfig(
            use_threads = True,
            max_concurrency = cls.settings.core.num_workers
        )
        def create_s3t():
            return s3transfer.create_transfer_manager(cls.boto, transfer_config)
        
        s3tm = create_s3t()

        # cls.s3t = create_s3t
        cls.s3t = s3tm
        cls.fs.s3tm = s3tm
        cls.fsa.s3tm = s3tm
    

    def build_wasabi(cls, **auth_config):
        LazyLib.import_lib('s3fs')
        LazyLib.import_lib('boto3')
        
        import s3fs
        import boto3
        import boto3.s3.transfer as s3transfer
        from botocore.config import Config as BotoConfig
        from fileio.providers.filesys.wasabi_s3 import WasabiFileSystem

        if auth_config: cls.settings.wasabi.update_auth(**auth_config)
        config = cls.settings.wasabi.build_s3fs_config()
        # cls.fs = s3fs.S3FileSystem(asynchronous=False, **config)
        # cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **config))
        cls.fs = WasabiFileSystem(asynchronous=False, **config)
        cls.fsa = rewrite_async_syntax(WasabiFileSystem(asynchronous=True, **config))
                
        boto_config = BotoConfig(
            max_pool_connections = cls.settings.core.num_workers * 2
        )
        cls.boto = boto3.client(
            's3', 
            region_name = cls.settings.wasabi.wasabi_region, 
            endpoint_url = cls.settings.wasabi.wasabi_endpoint, 
            aws_access_key_id = cls.settings.wasabi.wasabi_access_key_id, 
            aws_secret_access_key = cls.settings.wasabi.wasabi_secret_access_key, 
            config = boto_config
        )
        transfer_config = s3transfer.TransferConfig(
            use_threads = True,
            max_concurrency = cls.settings.core.num_workers
        )
        def create_s3t():
            return s3transfer.create_transfer_manager(cls.boto, transfer_config)
        
        # cls.s3t = create_s3t
        cls.s3t = property(create_s3t)


    def build_adlfs(cls, **auth_config):
        LazyLib.import_lib('adlfs')
        import adlfs
        if auth_config: cls.settings.azure.update_auth(**auth_config)
        config = cls.settings.azure.build_azurefs_config()
        cls.fs = adlfs.AzureBlobFileSystem(asynchronous = False, **config)
        cls.fsa = rewrite_async_syntax(adlfs.AzureBlobFileSystem(asynchronous=True, **config), 'az')
        

    def build_filesystems(self, force: bool = False, **auth_config):
        """
        Lazily inits the filesystems
        """
        if self.fs is not None and self.fsa is not None and not force: 
            return
        if self.fs_name == 's3fs':
            self.build_s3fs(**auth_config)
        elif self.fs_name == 'minio':
            self.build_minio(**auth_config)
        elif self.fs_name == 'gcsfs':
            self.build_gcsfs(**auth_config)
        elif self.fs_name == 's3c':
            self.build_s3c(**auth_config)
        elif self.fs_name == 'r2':
            self.build_r2(**auth_config)
        elif self.fs_name == 'wasabi':
            self.build_wasabi(**auth_config)
        elif self.fs_name == 'az':
            self.build_adlfs(**auth_config)


    @classmethod
    def reload_filesystem(cls):
        """ 
        Reinitializes the Filesystem
        """
        raise NotImplementedError


def _dummy_func(*args, **kwargs) -> Optional[Any]:
    pass

async def dummy_async_func(*args, **kwargs)  -> Optional[Any]:
    pass

def create_method_fs(cfs: Type[CloudFileSystemType], name: Union[str, List[str]],  func: Optional[Callable] = None, fs_type: str = 'fs') -> Optional[Callable]:
    if not hasattr(cfs, fs_type):
        #print(f'{cfs.__name__} has no {fs_type}')
        return _dummy_func
    fs_module = getattr(cfs, fs_type)
    if not isinstance(name, list): name = [name]
    for n in name:
        if hasattr(fs_module, n):
            #print(f'{cfs.__name__}:{fs_module} has func {fs_type}:{n}')
            return func(getattr(fs_module, n)) if func else getattr(fs_module, n)
    #print(f'{cfs.__name__} has no func {fs_type}:{name}')
    return _dummy_func

def create_async_method_fs(cfs: Type[CloudFileSystemType], name: Union[str, List[str]], func: Optional[Callable] = None, fs_type: str = 'fsa') -> Optional[Union[Callable, Coroutine]]:
    if not hasattr(cfs, fs_type):
        return dummy_async_func
    fs_module = getattr(cfs, fs_type)
    if not isinstance(name, list): name = [name]
    for n in name:
        if hasattr(fs_module, n):
            return func(getattr(fs_module, n)) if func else getattr(fs_module, n)
    return dummy_async_func

def create_staticmethod(cfs: Type[CloudFileSystemType], name: Union[str, List[str]], fs_type: str = 'fs'):
    return create_method_fs(cfs, name = name, func = staticmethod, fs_type = fs_type)


def create_async_coro(cfs: Type[CloudFileSystemType], name: Union[str, List[str]], fs_type: str = 'fs'):
    return create_async_method_fs(cfs, name = name, func = func_as_method_coro, fs_type = fs_type)


class BaseAccessor(NormalAccessor):
    """
    Dummy Accessor class
    """

    
    class CloudFileSystem(metaclass=CloudFileSystemType):
        pass
    
    info: Callable = create_staticmethod(CloudFileSystem, 'info')
    stat: Callable = create_staticmethod(CloudFileSystem, 'stat')
    size: Callable = create_staticmethod(CloudFileSystem, 'size')
    exists: Callable = create_staticmethod(CloudFileSystem, 'exists')
    is_dir: Callable = create_staticmethod(CloudFileSystem, 'isdir')
    is_file: Callable = create_staticmethod(CloudFileSystem, 'isfile')
    copy: Callable = create_staticmethod(CloudFileSystem, 'copy')
    copy_file: Callable = create_staticmethod(CloudFileSystem, 'cp_file')
    get_file: Callable = create_staticmethod(CloudFileSystem, 'get_file')
    put_file: Callable = create_staticmethod(CloudFileSystem, 'put_file')
    metadata: Callable = create_staticmethod(CloudFileSystem, ['metadata', 'info'])
    checksum: Callable = create_staticmethod(CloudFileSystem, 'checksum')

    get: Callable = create_staticmethod(CloudFileSystem, 'get')
    put: Callable = create_staticmethod(CloudFileSystem, 'put')

    open: Callable = create_method_fs(CloudFileSystem, 'open')
    listdir: Callable = create_staticmethod(CloudFileSystem, 'ls')    
    walk: Callable = create_staticmethod(CloudFileSystem, 'walk')

    glob: Callable = create_method_fs(CloudFileSystem, 'glob')
    find: Callable = create_method_fs(CloudFileSystem, 'find')
    touch: Callable = create_method_fs(CloudFileSystem, 'touch')
    cat: Callable = create_method_fs(CloudFileSystem, 'cat')
    cat_file: Callable = create_method_fs(CloudFileSystem, 'cat_file')
    
    
    pipe: Callable = create_method_fs(CloudFileSystem, 'pipe')
    pipe_file: Callable = create_method_fs(CloudFileSystem, 'pipe_file')
    
    mkdir: Callable = create_method_fs(CloudFileSystem, 'mkdir')
    makedirs: Callable = create_method_fs(CloudFileSystem, ['makedirs', 'mkdirs'])
    unlink: Callable = create_method_fs(CloudFileSystem, 'rm_file')
    rmdir: Callable = create_method_fs(CloudFileSystem, 'rmdir')
    rename: Callable = create_method_fs(CloudFileSystem, 'rename')
    replace: Callable = create_method_fs(CloudFileSystem, 'rename')
    remove: Callable = create_method_fs(CloudFileSystem, 'rm')
    rm: Callable = create_staticmethod(CloudFileSystem, 'rm')
    rm_file: Callable = create_staticmethod(CloudFileSystem, 'rm_file')
    
    modified: Callable = create_method_fs(CloudFileSystem, 'modified')
    url: Callable = create_method_fs(CloudFileSystem, 'url')
    ukey: Callable = create_method_fs(CloudFileSystem, 'ukey')
    setxattr: Callable = create_method_fs(CloudFileSystem, 'setxattr')
    invalidate_cache: Callable = create_method_fs(CloudFileSystem, 'invalidate_cache')
    
    filesys: 'AsyncFileSystem' = CloudFileSystem.fs
    async_filesys: 'AsyncFileSystem' = CloudFileSystem.fsa

    boto: ClassVar = CloudFileSystem.boto
    s3t: 'TransferManager' = CloudFileSystem.s3t
    tffs: 'tfFS' = CloudFileSystem.tffs
    
    # Async Methods
    async_stat: Callable = create_async_coro(CloudFileSystem, 'stat')
    async_touch: Callable = create_async_coro(CloudFileSystem, 'touch')
    async_ukey: Callable = create_async_coro(CloudFileSystem, 'ukey')
    async_size: Callable = create_async_coro(CloudFileSystem, 'size')
    async_url: Callable = create_async_coro(CloudFileSystem, 'url')
    async_setxattr: Callable = create_async_coro(CloudFileSystem, 'setxattr')
    async_modified: Callable = create_async_coro(CloudFileSystem, 'modified')
    async_invalidate_cache: Callable = create_async_coro(CloudFileSystem, 'invalidate_cache')
    async_rename: Callable = create_async_coro(CloudFileSystem, 'rename')
    async_replace: Callable = create_async_coro(CloudFileSystem, 'rename')

    async_info: Callable = create_async_method_fs(CloudFileSystem, 'async_info')
    async_exists: Callable = create_async_method_fs(CloudFileSystem, 'async_exists')
    async_glob: Callable = create_async_method_fs(CloudFileSystem, 'async_glob')
    async_find: Callable = create_async_method_fs(CloudFileSystem, 'async_find')
    async_is_dir: Callable = create_async_method_fs(CloudFileSystem, 'async_isdir')
    async_is_file: Callable = create_async_method_fs(CloudFileSystem, 'async_is_file')
    async_copy: Callable = create_async_method_fs(CloudFileSystem, 'async_copy')
    async_copy_file: Callable = create_async_method_fs(CloudFileSystem, 'async_cp_file')

    async_pipe: Callable = create_async_method_fs(CloudFileSystem, 'async_pipe')
    async_pipe_file: Callable = create_async_method_fs(CloudFileSystem, 'async_pipe_file')

    async_get: Callable = create_async_coro(CloudFileSystem, 'async_get')
    async_get_file: Callable = create_async_coro(CloudFileSystem, 'async_get_file')
    
    async_put: Callable = create_async_method_fs(CloudFileSystem, 'async_put')
    async_put_file: Callable = create_async_method_fs(CloudFileSystem, 'async_put_file')
    async_metadata: Callable = create_async_method_fs(CloudFileSystem, 'async_info')
    async_open: Callable = create_async_method_fs(CloudFileSystem, '_open')
    async_mkdir: Callable = create_async_method_fs(CloudFileSystem, 'async_mkdir')
    async_makedirs: Callable = create_async_method_fs(CloudFileSystem, 'async_makedirs')
    async_unlink: Callable = create_async_method_fs(CloudFileSystem, 'async_rm_file')
    async_rmdir: Callable = create_async_method_fs(CloudFileSystem, 'async_rmdir')
    async_remove: Callable = create_async_method_fs(CloudFileSystem, 'async_rm')
    async_rm: Callable = create_async_method_fs(CloudFileSystem, 'async_rm')
    async_rm_file: Callable = create_async_coro(CloudFileSystem, 'async_rm_file')
    async_listdir: Callable = create_async_method_fs(CloudFileSystem, ['async_listdir', 'async_list_objects'])
    async_walk: Callable = create_async_method_fs(CloudFileSystem, 'async_walk')

    @classmethod
    def reload_cfs(cls, **kwargs):
        cls.CloudFileSystem.build_filesystems(**kwargs)
        cls.info: Callable = create_staticmethod(cls.CloudFileSystem, 'info')
        cls.stat: Callable = create_staticmethod(cls.CloudFileSystem, 'stat')
        cls.size: Callable = create_staticmethod(cls.CloudFileSystem, 'size')
        cls.exists: Callable = create_staticmethod(cls.CloudFileSystem, 'exists')
        cls.is_dir: Callable = create_staticmethod(cls.CloudFileSystem, 'isdir')
        cls.is_file: Callable = create_staticmethod(cls.CloudFileSystem, 'isfile')
        cls.copy: Callable = create_staticmethod(cls.CloudFileSystem, 'copy')
        cls.copy_file: Callable = create_staticmethod(cls.CloudFileSystem, 'cp_file')
        cls.get_file: Callable = create_staticmethod(cls.CloudFileSystem, 'get_file')
        cls.put_file: Callable = create_staticmethod(cls.CloudFileSystem, 'put_file')
        cls.metadata: Callable = create_staticmethod(cls.CloudFileSystem, ['metadata', 'info'])

        cls.open: Callable = create_method_fs(cls.CloudFileSystem, 'open')
        cls.listdir: Callable = create_staticmethod(cls.CloudFileSystem, 'ls')    
        cls.walk: Callable = create_staticmethod(cls.CloudFileSystem, 'walk')
        cls.glob: Callable = create_staticmethod(cls.CloudFileSystem, 'glob')
        cls.get: Callable = create_staticmethod(cls.CloudFileSystem, 'get')
        cls.put: Callable = create_staticmethod(cls.CloudFileSystem, 'put')
        
        cls.checksum: Callable = create_method_fs(cls.CloudFileSystem, 'checksum')
        cls.cat: Callable = create_staticmethod(cls.CloudFileSystem, 'cat')
        cls.cat_file: Callable = create_staticmethod(cls.CloudFileSystem, 'cat_file')
        
        cls.pipe: Callable = create_staticmethod(cls.CloudFileSystem, 'pipe')
        cls.pipe_file: Callable = create_staticmethod(cls.CloudFileSystem, 'pipe_file')
    

        cls.find: Callable = create_method_fs(cls.CloudFileSystem, 'find')
        cls.touch: Callable = create_method_fs(cls.CloudFileSystem, 'touch')
        
        
        cls.mkdir: Callable = create_method_fs(cls.CloudFileSystem, 'mkdir')
        cls.makedirs: Callable = create_method_fs(cls.CloudFileSystem, ['makedirs', 'mkdirs'])
        cls.unlink: Callable = create_method_fs(cls.CloudFileSystem, 'rm_file')
        cls.rmdir: Callable = create_method_fs(cls.CloudFileSystem, 'rmdir')
        cls.rename : Callable = create_method_fs(cls.CloudFileSystem, 'rename')
        cls.replace : Callable = create_method_fs(cls.CloudFileSystem, 'rename')
        cls.rm : Callable = create_staticmethod(cls.CloudFileSystem, 'rm')
        cls.rm_file : Callable = create_staticmethod(cls.CloudFileSystem, 'rm_file')
        
        cls.remove : Callable = create_method_fs(cls.CloudFileSystem, 'rm')
        cls.modified: Callable = create_method_fs(cls.CloudFileSystem, 'modified')
        cls.setxattr: Callable = create_method_fs(cls.CloudFileSystem, 'setxattr')
        cls.url: Callable = create_method_fs(cls.CloudFileSystem, 'url')
        cls.ukey: Callable = create_method_fs(cls.CloudFileSystem, 'ukey')
        cls.invalidate_cache: Callable = create_method_fs(cls.CloudFileSystem, 'invalidate_cache')
        
        cls.filesys = cls.CloudFileSystem.fs
        cls.async_filesys = cls.CloudFileSystem.fsa
        cls.boto = cls.CloudFileSystem.boto
        cls.s3t = cls.CloudFileSystem.s3t
        cls.tffs = cls.CloudFileSystem.tffs

        
        # Async Methods
        cls.async_stat: Callable = create_async_coro(cls.CloudFileSystem, 'stat')
        cls.async_touch: Callable = create_async_coro(cls.CloudFileSystem, 'touch')
        cls.async_ukey: Callable = create_async_coro(cls.CloudFileSystem, 'ukey')
        cls.async_size: Callable = create_async_coro(cls.CloudFileSystem, 'size')
        cls.async_url: Callable = create_async_coro(cls.CloudFileSystem, 'url')
        cls.async_setxattr: Callable = create_async_coro(cls.CloudFileSystem, 'setxattr')

        cls.async_modified: Callable = create_async_coro(cls.CloudFileSystem, 'modified')
        cls.async_invalidate_cache: Callable = create_async_coro(cls.CloudFileSystem, 'invalidate_cache')
        cls.async_rename: Callable = create_async_coro(cls.CloudFileSystem, 'rename')
        cls.async_replace: Callable = create_async_coro(cls.CloudFileSystem, 'rename')

        cls.async_info: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_info')
        cls.async_exists: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_exists')

        cls.async_glob: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_glob')
        cls.async_find: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_find')
        
        cls.async_cat: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_cat')
        cls.async_cat_file: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_cat_file')
        
        cls.async_pipe: Callable = create_async_coro(cls.CloudFileSystem, 'async_pipe')
        cls.async_pipe_file: Callable = create_async_coro(cls.CloudFileSystem, 'async_pipe_file')
        
        cls.async_is_dir: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_isdir')
        cls.async_is_file: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_isfile')
        cls.async_copy: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_copy')
        cls.async_copy_file: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_cp_file')
        cls.async_get: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_get')
        cls.async_get_file: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_get_file')
        cls.async_put: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_put')
        cls.async_put_file: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_put_file')
        cls.async_metadata: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_info')
        cls.async_open: Callable = create_async_method_fs(cls.CloudFileSystem, '_open')
        cls.async_mkdir: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_mkdir')
        cls.async_makedirs: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_makedirs')
        cls.async_unlink: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_rm_file')
        cls.async_rm_file: Callable = create_async_coro(cls.CloudFileSystem, 'async_rm_file')
        cls.async_rmdir: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_rmdir')
        cls.async_remove: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_rm')
        cls.async_rm: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_rm')
        cls.async_listdir: Callable = create_async_method_fs(cls.CloudFileSystem, ['async_listdir', 'async_list_objects'])
        cls.async_walk: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_walk')

