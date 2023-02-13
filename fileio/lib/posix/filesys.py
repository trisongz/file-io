
import contextlib
from typing import Callable, Any, Optional, Union, Dict, Type, TYPE_CHECKING

from fileio.lib.posix.meta import CloudFileSystemType, BaseAccessor

if TYPE_CHECKING:
    from fileio.providers.tfio import tfFS
    with contextlib.suppress(ImportError):
        import gcsfs
    with contextlib.suppress(ImportError):
        import s3fs
    with contextlib.suppress(ImportError):
        import boto3
    # with contextlib.suppress(ImportError):
    #     from fileio.providers.hffs.filesys import HfFileSystem


class GCP_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 'gcsfs.GCSFileSystem' = None
    fsa: 'gcsfs.GCSFileSystem' = None
    fs_name: str = 'gcsfs'
    tffs: 'tfFS' = None # Tensorflow IO

class AWS_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 's3fs'
    boto: 'boto3.session.Session' = None
    s3t: Callable = None

class Minio_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 'minio'
    boto: 'boto3.session.Session' = None
    s3t: Callable = None

class S3Compat_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 's3c'
    boto: 'boto3.session.Session' = None
    s3t: Callable = None

# class HF_CloudFileSystem(metaclass=CloudFileSystemType):
#     fs: 'HfFileSystem' = None
#     fsa: 'HfFileSystem' = None
#     fs_name: str = 'hf'

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

class S3Compat_Accessor(BaseAccessor):
    """
    S3 Filelike Accessor that inherits from BaseAccessor
    """
    class CloudFileSystem(S3Compat_CloudFileSystem):
        pass

# class HF_Accessor(BaseAccessor):
#     """
#     HF Filelike Accessor that inherits from BaseAccessor
#     """
#     class CloudFileSystem(HF_CloudFileSystem):
#         pass


AccessorLike = Union[
    BaseAccessor,
    GCP_Accessor,
    AWS_Accessor,
    Minio_Accessor,
    S3Compat_Accessor,
    # HF_Accessor,
]

CloudFileSystemLike = Union[
    CloudFileSystemType,
    GCP_CloudFileSystem,
    AWS_CloudFileSystem,
    Minio_CloudFileSystem,
    S3Compat_CloudFileSystem,
    # HF_CloudFileSystem,
]

class AccessorMeta(type):
    """
    AccessorMeta is a metaclass that is used to register file systems
    """

    ax: Dict[str, Union[AccessorLike, Any]] = {}
    fs_map: Dict[str, Type[CloudFileSystemLike]] = {
        'gs': GCP_CloudFileSystem,
        's3': AWS_CloudFileSystem,
        'minio': Minio_CloudFileSystem,
        's3c': S3Compat_CloudFileSystem,
        # 'hf': HF_CloudFileSystem,
    }

    @classmethod
    def get_gs_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> GCP_Accessor:
        if not cls.ax.get('gs') or _reset:
            GCP_CloudFileSystem.build_filesystems(**kwargs)
            GCP_Accessor.reload_cfs(**kwargs)
            cls.ax['gs'] = GCP_Accessor()
        return cls.ax['gs']
    
    @classmethod
    def get_s3_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> AWS_Accessor:
        if not cls.ax.get('s3') or _reset:
            AWS_CloudFileSystem.build_filesystems(**kwargs)
            AWS_Accessor.reload_cfs(**kwargs)
            cls.ax['s3'] = AWS_Accessor()
        return cls.ax['s3']
    
    @classmethod
    def get_minio_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> Minio_Accessor:
        if not cls.ax.get('minio') or _reset:
            Minio_CloudFileSystem.build_filesystems(**kwargs)
            Minio_Accessor.reload_cfs(**kwargs)
            cls.ax['minio'] = Minio_Accessor()
        return cls.ax['minio']
    
    @classmethod
    def get_s3c_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> S3Compat_Accessor:
        if not cls.ax.get('s3c') or _reset:
            S3Compat_CloudFileSystem.build_filesystems(**kwargs)
            S3Compat_Accessor.reload_cfs(**kwargs)
            cls.ax['s3c'] = S3Compat_Accessor()
        return cls.ax['s3c']

    @classmethod
    def get_accessor(
        cls, 
        name: str, 
        _reset: Optional[bool] = False, 
        **kwargs
    ) -> AccessorLike:
        """
        Returns an accessor for the given file system name
        """
        _ax = getattr(cls, f'get_{name}_accessor', None)
        return _ax(_reset=_reset, **kwargs) if _ax else BaseAccessor
    
    @classmethod
    def get_fs(
        cls, 
        name: str, 
        _reset: Optional[bool] = False, 
        **kwargs
    ) -> CloudFileSystemLike:
        """
        Returns a file system for the given file system name
        """
        _fs: CloudFileSystemLike = cls.fs_map.get(name, None)
        if not _fs: return CloudFileSystemType
        if _reset:
            _fs.build_filesystems(**kwargs)
        return _fs
        

class FileSysManager(metaclass=AccessorMeta):
    pass

