from __future__ import annotations

import atexit
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
    with contextlib.suppress(ImportError):
        import adlfs
    with contextlib.suppress(ImportError):
        from fileio.providers.filesys.cloudflare_r2 import R2FileSystem
    with contextlib.suppress(ImportError):
        from fileio.providers.filesys.wasabi_s3 import WasabiFileSystem
    
    with contextlib.suppress(ImportError):
        from s3transfer.manager import TransferManager


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
    s3t: 'TransferManager' = None


class Minio_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 'minio'
    boto: 'boto3.session.Session' = None
    s3t: 'TransferManager' = None

class S3Compat_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 's3c'
    boto: 'boto3.session.Session' = None
    s3t: 'TransferManager' = None

    # async def _aonexit(self, *args, **kwargs):
    #     """
    #     On Exit Function
    #     """
    #     await self.fs.s3.close()
    #     await self.fsa.s3.close()

    # def _onexit(self, *args, **kwargs):
    #     """
    #     On Exit Function
    #     """
    #     sync(
    #         self.fs.loop,
    #         self.fs.s3.close
    #     )
    #     sync(
    #         self.fsa.loop,
    #         self.fsa.s3.close
    #     )
    #     # self.fs.s3.close()
    #     # self.fsa.s3.close()

class R2_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 'R2FileSystem' = None
    fsa: 'R2FileSystem' = None
    fs_name: str = 'r2'
    boto: 'boto3.session.Session' = None
    s3t: 'TransferManager' = None

class Wasabi_CloudFileSystem(metaclass=CloudFileSystemType):
    # fs: 's3fs.S3FileSystem' = None
    # fsa: 's3fs.S3FileSystem' = None
    fs: 'WasabiFileSystem' = None
    fsa: 'WasabiFileSystem' = None
    fs_name: str = 'wasabi'
    boto: 'boto3.session.Session' = None
    s3t: 'TransferManager' = None

class Azure_CloudFileSystem(metaclass=CloudFileSystemType):
    fs: 'adlfs.AzureBlobFileSystem' = None
    fsa: 'adlfs.AzureBlobFileSystem' = None
    fs_name: str = 'azure'

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

class R2_Accessor(BaseAccessor):
    """
    R2 Filelike Accessor that inherits from BaseAccessor
    """
    class CloudFileSystem(R2_CloudFileSystem):
        pass


class Wasabi_Accessor(BaseAccessor):
    """
    Wasabi Filelike Accessor that inherits from BaseAccessor
    """
    class CloudFileSystem(Wasabi_CloudFileSystem):
        pass


class Azure_Accessor(BaseAccessor):
    """
    Azure Filelike Accessor that inherits from BaseAccessor
    """
    class CloudFileSystem(Azure_CloudFileSystem):
        pass

AccessorLike = Union[
    BaseAccessor,
    GCP_Accessor,
    AWS_Accessor,
    Minio_Accessor,
    S3Compat_Accessor,
    # HF_Accessor,
    R2_Accessor,
    Wasabi_Accessor,
    Azure_Accessor,
]

CloudFileSystemLike = Union[
    CloudFileSystemType,
    GCP_CloudFileSystem,
    AWS_CloudFileSystem,
    Minio_CloudFileSystem,
    S3Compat_CloudFileSystem,
    # HF_CloudFileSystem,
    R2_CloudFileSystem,
    Wasabi_CloudFileSystem,
    Azure_CloudFileSystem,
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
        'r2': R2_CloudFileSystem,
        'wsbi': Wasabi_CloudFileSystem,
        'az': Azure_CloudFileSystem,

    }

    def get_gs_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> GCP_Accessor:
        if not cls.ax.get('gs') or _reset:
            GCP_CloudFileSystem.build_filesystems(**kwargs)
            GCP_Accessor.reload_cfs(**kwargs)
            cls.ax['gs'] = GCP_Accessor()
        return cls.ax['gs']
    
    def get_s3_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> AWS_Accessor:
        if not cls.ax.get('s3') or _reset:
            AWS_CloudFileSystem.build_filesystems(**kwargs)
            AWS_Accessor.reload_cfs(**kwargs)
            cls.ax['s3'] = AWS_Accessor()
            # atexit.register(AWS_CloudFileSystem._onexit)
        return cls.ax['s3']
    
    def get_minio_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> Minio_Accessor:
        if not cls.ax.get('minio') or _reset:
            Minio_CloudFileSystem.build_filesystems(**kwargs)
            Minio_Accessor.reload_cfs(**kwargs)
            cls.ax['minio'] = Minio_Accessor()
        return cls.ax['minio']
    
    def get_s3c_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> S3Compat_Accessor:
        if not cls.ax.get('s3c') or _reset:
            S3Compat_CloudFileSystem.build_filesystems(**kwargs)
            S3Compat_Accessor.reload_cfs(**kwargs)
            cls.ax['s3c'] = S3Compat_Accessor()
        return cls.ax['s3c']

    def get_r2_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> R2_Accessor:
        if not cls.ax.get('r2') or _reset:
            R2_CloudFileSystem.build_filesystems(**kwargs)
            R2_Accessor.reload_cfs(**kwargs)
            cls.ax['r2'] = R2_Accessor()
        return cls.ax['r2']

    def get_wsbi_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> Wasabi_Accessor:
        if not cls.ax.get('wsbi') or _reset:
            Wasabi_CloudFileSystem.build_filesystems(**kwargs)
            Wasabi_Accessor.reload_cfs(**kwargs)
            cls.ax['wsbi'] = Wasabi_Accessor()
        return cls.ax['wsbi']
    
    def get_az_accessor(cls, _reset: Optional[bool] = False, **kwargs) -> Azure_Accessor:
        if not cls.ax.get('az') or _reset:
            Azure_CloudFileSystem.build_filesystems(**kwargs)
            Azure_Accessor.reload_cfs(**kwargs)
            cls.ax['az'] = Azure_Accessor()
        return cls.ax['az']

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
    

    def get_fs(
        cls, 
        name: str, 
        _reset: Optional[bool] = False, 
        **kwargs
    ) -> CloudFileSystemLike:
        """
        Returns a file system for the given file system name
        """
        _fs: CloudFileSystemLike = cls.fs_map.get(name)
        if not _fs: return CloudFileSystemType
        if _reset: _fs.build_filesystems(**kwargs)
        return _fs
        

class FileSysManager(metaclass=AccessorMeta):
    pass

