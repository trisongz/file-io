from types import ModuleType
from typing import Callable, Any, Optional, Coroutine, Type, Union, List, ClassVar

try: import gcsfs
except ImportError: gcsfs: ModuleType = None

try: import s3fs
except ImportError: s3fs: ModuleType = None

from .base import rewrite_async_syntax, NormalAccessor, func_as_method_coro


class CloudFileSystemType(type):
    fs: ModuleType = None
    fsa: ModuleType = None
    fs_name: str = None # gcsfs

    @classmethod
    def is_ready(cls):
        return bool(cls.fsa and cls.fs)
    
    #@classmethod
    def build_gcsfs(cls, **auth_config):
        from .._modules import LibModule
        from .config import CloudConfig

        gcsfs: ModuleType = LibModule.import_lib('gcsfs')
        LibModule.reload_module(gcsfs)

        authz = CloudConfig()
        if auth_config: authz.update_auth(**auth_config)
        _config = {}
        if authz.google_application_credentials.exists: _config['token'] = authz.google_application_credentials.as_posix()
        gcp_project = authz.get_gcp_project()
        if gcp_project: _config['project'] = gcp_project
        if authz.gcs_client_config: _config['client_kwargs'] = authz.gcs_client_config
        if authz.gcs_config: _config['config_kwargs'] = authz.gcs_config
        cls.fs = gcsfs.GCSFileSystem(asynchronous=False, **_config)
        cls.fsa = rewrite_async_syntax(gcsfs.GCSFileSystem(asynchronous=True, **_config), 'gs')
    
    #@classmethod
    def build_s3fs(cls, **auth_config):
        from .._modules import LibModule
        from .config import CloudConfig

        s3fs = LibModule.import_lib('s3fs')
        LibModule.reload_module(s3fs)

        authz = CloudConfig()
        if auth_config: authz.update_auth(**auth_config)
        _config = {}
        if authz.aws_access_key_id:
            _config['key'] = authz.aws_access_key_id
            _config['secret'] = authz.aws_secret_access_key
        elif authz.aws_access_token: _config['token'] = authz.aws_access_token
        elif not authz.boto_config: _config['anon'] = True
        if authz.s3_config: _config['config_kwargs'] = authz.s3_config
        cls.fs = s3fs.S3FileSystem(asynchronous=False, **_config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))
    
    #classmethod
    def build_minio(cls, **auth_config):
        from .._modules import LibModule
        from .config import CloudConfig

        s3fs: ModuleType = LibModule.import_lib('s3fs')
        LibModule.reload_module(s3fs)

        authz = CloudConfig()
        if auth_config: authz.update_auth(**auth_config)
        _config = {}
        
        if authz.minio_secret_key:
            _config['key'] = authz.minio_access_key
            _config['secret'] = authz.minio_secret_key
        elif authz.minio_access_token: _config['token'] = authz.minio_access_token
        _config['client_kwargs'] = {'endpoint_url': authz.minio_endpoint, 'region_name': authz.aws_region}
        _config['config_kwargs'] = authz.minio_config or {}
        _config['config_kwargs']['signature_version'] = authz.minio_signature_ver
        cls.fs = s3fs.S3FileSystem(asynchronous=False, **_config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))

    #@classmethod
    def build_filesystems(cls, force: bool = False, **auth_config):
        """
        Lazily inits the filesystems
        """
        if cls.fs and cls.fsa and not force: return
        if cls.fs_name == 's3fs':
            cls.build_s3fs(**auth_config)
        elif cls.fs_name == 'minio':
            cls.build_minio(**auth_config)
        elif cls.fs_name == 'gcsfs':
            cls.build_gcsfs(**auth_config)


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
            if func: return func(getattr(fs_module, n))
            return getattr(fs_module, n)
    #print(f'{cfs.__name__} has no func {fs_type}:{name}')
    return _dummy_func

def create_async_method_fs(cfs: Type[CloudFileSystemType], name: Union[str, List[str]], func: Optional[Callable] = None, fs_type: str = 'fsa') -> Optional[Union[Callable, Coroutine]]:
    if not hasattr(cfs, fs_type):
        return dummy_async_func
    fs_module = getattr(cfs, fs_type)
    if not isinstance(name, list): name = [name]
    for n in name:
        if hasattr(fs_module, n):
            if func: return func(getattr(fs_module, n))
            return getattr(fs_module, n)
    return dummy_async_func

def create_staticmethod(cfs: Type[CloudFileSystemType], name: Union[str, List[str]], fs_type: str = 'fs'):
    return create_method_fs(cfs, name = name, func = staticmethod, fs_type = fs_type)


def create_async_coro(cfs: Type[CloudFileSystemType], name: Union[str, List[str]], fs_type: str = 'fs'):
    return create_async_method_fs(cfs, name = name, func = func_as_method_coro, fs_type = fs_type)


class BaseAccessor(NormalAccessor):
    """Dummy Accessor class
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
    listdir: Callable = create_method_fs(CloudFileSystem, 'ls')    
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
    rename : Callable = create_method_fs(CloudFileSystem, 'rename')
    replace : Callable = create_method_fs(CloudFileSystem, 'rename')
    remove : Callable = create_method_fs(CloudFileSystem, 'rm')
    rm : Callable = create_staticmethod(CloudFileSystem, 'rm')
    rm_file : Callable = create_staticmethod(CloudFileSystem, 'rm_file')
    
    modified: Callable = create_method_fs(CloudFileSystem, 'modified')
    url: Callable = create_method_fs(CloudFileSystem, 'url')
    ukey: Callable = create_method_fs(CloudFileSystem, 'ukey')
    setxattr: Callable = create_method_fs(CloudFileSystem, 'setxattr')
    invalidate_cache: Callable = create_method_fs(CloudFileSystem, 'invalidate_cache')
    
    filesys: ClassVar = CloudFileSystem.fs
    async_filesys: ClassVar = CloudFileSystem.fsa
    
    # Async Methods
    async_stat: Callable = create_async_coro(CloudFileSystem, 'stat')
    async_touch: Callable = create_async_coro(CloudFileSystem, 'touch')
    async_ukey: Callable = create_async_coro(CloudFileSystem, 'ukey')
    async_size: Callable = create_async_coro(CloudFileSystem, 'size')
    async_url: Callable = create_async_coro(CloudFileSystem, 'url')
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
        cls.listdir: Callable = create_method_fs(cls.CloudFileSystem, 'ls')    
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
        
        # Async Methods
        cls.async_stat: Callable = create_async_coro(cls.CloudFileSystem, 'stat')
        cls.async_touch: Callable = create_async_coro(cls.CloudFileSystem, 'touch')
        cls.async_ukey: Callable = create_async_coro(cls.CloudFileSystem, 'ukey')
        cls.async_size: Callable = create_async_coro(cls.CloudFileSystem, 'size')
        cls.async_url: Callable = create_async_coro(cls.CloudFileSystem, 'url')
        cls.async_modified: Callable = create_async_coro(cls.CloudFileSystem, 'modified')
        cls.async_invalidate_cache: Callable = create_async_coro(cls.CloudFileSystem, 'invalidate_cache')
        cls.async_rename: Callable = create_async_coro(cls.CloudFileSystem, 'rename')
        cls.async_replace: Callable = create_async_coro(cls.CloudFileSystem, 'rename')

        cls.async_info: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_info')
        cls.async_exists: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_exists')

        cls.async_glob: Callable = create_async_coro(cls.CloudFileSystem, 'async_glob')
        cls.async_glob: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_find')
        
        cls.async_cat: Callable = create_async_coro(cls.CloudFileSystem, 'async_cat')
        cls.async_cat_file: Callable = create_async_coro(cls.CloudFileSystem, 'async_cat_file')
        
        cls.async_pipe: Callable = create_async_coro(cls.CloudFileSystem, 'async_pipe')
        cls.async_pipe_file: Callable = create_async_coro(cls.CloudFileSystem, 'async_pipe_file')
        
        cls.async_is_dir: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_isdir')
        cls.async_is_file: Callable = create_async_method_fs(cls.CloudFileSystem, 'async_is_file')
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



class GCP_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 'gcsfs.GCSFileSystem' = None
    fsa: 'gcsfs.GCSFileSystem' = None
    fs_name: str = 'gcsfs'

class AWS_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 's3fs'

class Minio_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 'minio'


class GCP_Accessor(BaseAccessor):
    """
    GCP Filelike Accessor that inherits from BaseAccessor
    """
    class CloudFileSystem(GCP_CloudFileSystem):
        pass

class AWS_Accessor(BaseAccessor):
    """
    AWS Filelike Accessor that inherits from BaseAccessor
    """
    class CloudFileSystem(AWS_CloudFileSystem):
        pass

class Minio_Accessor(BaseAccessor):
    """
    S3 Filelike Accessor that inherits from BaseAccessor
    """
    class CloudFileSystem(Minio_CloudFileSystem):
        pass

_GCPAccessor: GCP_Accessor = None
_AWSAccessor: AWS_Accessor = None
_MinioAccessor: Minio_Accessor = None

def _get_gcp_accessor(**kwargs) -> GCP_Accessor:
    global _GCPAccessor, GCP_Accessor
    if not _GCPAccessor:
        GCP_CloudFileSystem.build_filesystems(**kwargs)
        GCP_Accessor.reload_cfs(**kwargs)
        _GCPAccessor = GCP_Accessor()
    return _GCPAccessor

def _get_aws_accessor(**kwargs) -> AWS_Accessor:
    global _AWSAccessor, AWS_Accessor
    if not _AWSAccessor:
        AWS_CloudFileSystem.build_filesystems(**kwargs)
        AWS_Accessor.reload_cfs(**kwargs)
        _AWSAccessor = AWS_Accessor()
    return _AWSAccessor

def _get_minio_accessor(**kwargs) -> Minio_Accessor:
    global _MinioAccessor, Minio_Accessor
    if not _MinioAccessor:
        Minio_CloudFileSystem.build_filesystems(**kwargs)
        Minio_Accessor.reload_cfs(**kwargs)
        _MinioAccessor = Minio_Accessor()
    return _MinioAccessor

_accessor_getters = {
    'gs': _get_gcp_accessor,
    's3': _get_aws_accessor,
    'minio': _get_minio_accessor
}
_cfs_getters = {
    'gs': GCP_CloudFileSystem,
    's3': AWS_CloudFileSystem,
    'minio': Minio_CloudFileSystem
}

AccessorLike = Union[
    BaseAccessor,
    GCP_Accessor,
    AWS_Accessor,
    Minio_Accessor
]
CloudFileSystemLike = Union[
    CloudFileSystemType,
    GCP_CloudFileSystem,
    AWS_CloudFileSystem,
    Minio_CloudFileSystem
]

def get_accessor(name: str, **kwargs) -> AccessorLike:
    if not _accessor_getters.get(name, None): return BaseAccessor
    return _accessor_getters[name](**kwargs)

def get_cloud_filesystem(name: str) -> Optional[CloudFileSystemLike]:
    return _cfs_getters.get(name, None)

