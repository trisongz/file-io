from __future__ import annotations

import os
import contextlib
from fileio.lib.posix.base import *
from fileio.lib.posix.filesys import R2_CloudFileSystem, R2_Accessor
from fileio.lib.posix.cloud import *
from fileio.utils import logger
from typing import Mapping

class FileR2PurePath(CloudFileSystemPurePath):
    _prefix: str = 'r2'
    _provider: str = 'CloudFlare'
    _win_pathz: ClassVar = 'PureFileR2WindowsPath'
    _posix_pathz: ClassVar = 'PureFileR2PosixPath'


class PureFileR2PosixPath(PureCloudFileSystemPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PureFileR2WindowsPath(PureCloudFileSystemWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()

with contextlib.suppress(ImportError):
    import s3fs

# https://developers.cloudflare.com/r2/api/s3/api/

class R2File(s3fs.S3File):


    def _call_s3(self, method, *kwarglist, **kwargs):
        """
        Filter out ACL for methods that we know will fail
        """
        # if method in ["create_multipart_upload", "put_object", "put_object_acl"]:
        kwargs.pop("ACL", None)
        return self.fs.call_s3(method, self.s3_additional_kwargs, *kwarglist, **kwargs)

    def _initiate_upload(self):
        if self.autocommit and not self.append_block and self.tell() < self.blocksize:
            # only happens when closing small file, use on-shot PUT
            return
        logger.info(f"Initiate upload for {self}")
        self.parts = []
        self.mpu = self._call_s3(
            "create_multipart_upload",
            Bucket=self.bucket,
            Key=self.key,
            # ACL=self.acl,
        )

        if self.append_block:
            # use existing data in key when appending,
            # and block is big enough
            out = self._call_s3(
                "upload_part_copy",
                self.s3_additional_kwargs,
                Bucket=self.bucket,
                Key=self.key,
                PartNumber=1,
                UploadId=self.mpu["UploadId"],
                CopySource=self.path,
            )
            self.parts.append({"PartNumber": 1, "ETag": out["CopyPartResult"]["ETag"]})


class FileR2Path(CloudFileSystemPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: R2_Accessor = None
    _pathlike = posixpath
    _prefix = 'r2'
    _provider = 'CloudFlare'

    _win_pathz: ModuleType = 'FileR2WindowsPath'
    _posix_pathz: ModuleType = 'FileR2PosixPath'

    def _init(self, template: Optional['FileR2Path'] = None):
        self._accessor: R2_Accessor = FileSysManager.get_accessor(self._prefix)
        # self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is FileR2Path or issubclass(cls, FileR2Path): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self
    

    def open(
        self, 
        mode: FileMode = 'r', 
        buffering: int = -1, 
        encoding: Optional[str] = DEFAULT_ENCODING, 
        errors: Optional[str] = ON_ERRORS, 
        newline: Optional[str] = NEWLINE, 
        # acl: Optional[str] = None,
        version_id: Optional[str] = None,
        fill_cache: Optional[bool] = True,
        block_size: Optional[int] = 5242880, 
        requester_pays: Optional[bool] = None,
        cache_type: Optional[str] = None,
        cache_options: Optional[Mapping[str, Any]] = None,
        compression: Optional[str] = None, 
        autocommit: Optional[bool] = True,

        **kwargs: Any
    ) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """

        # match the open method
        if block_size is None:
            block_size = self._accessor.filesys.default_block_size
        if fill_cache is None:
            fill_cache = self._accessor.filesys.default_fill_cache
        if requester_pays is None:
            requester_pays = bool(self._accessor.filesys.req_kw)

        # acl = acl or self._accessor.filesys.s3_additional_kwargs.get("ACL", "")
        kw = self._accessor.filesys.s3_additional_kwargs.copy()
        kw.update(kwargs)
        kw['encoding'] = encoding
        kw['errors'] = errors
        kw['newline'] = newline
        kw['buffering'] = buffering
        kw['compression'] = compression

        if not self._accessor.filesys.version_aware and version_id:
            raise ValueError(
                "version_id cannot be specified if the filesystem "
                "is not version aware"
            )

        if cache_type is None:
            cache_type = self._accessor.filesys.default_cache_type
        
        return R2File(
            self._accessor.filesys,
            self._cloudpath,
            mode,
            block_size=block_size,
            acl=None,
            version_id=version_id,
            fill_cache=fill_cache,
            s3_additional_kwargs=kw,
            cache_type=cache_type,
            autocommit=autocommit,
            requester_pays=requester_pays,
            cache_options=cache_options,
            # encoding=encoding,
            # errors=errors,
            # newline=newline,
            # buffering=buffering,
            # compression=compression,
            # **kwargs,
        )


        # return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, acl = acl, version_id = version_id, fill_cache = fill_cache, block_size=block_size, compression=compression, **kwargs)

    def async_open(
        self,
        mode: FileMode = 'r', 
        buffering: int = -1, 
        encoding: Optional[str] = DEFAULT_ENCODING, 
        errors: Optional[str] = ON_ERRORS, 
        newline: Optional[str] = NEWLINE, 
        # acl: Optional[str] = None,
        version_id: Optional[str] = None,
        fill_cache: Optional[bool] = True,
        block_size: Optional[int] = 5242880, 
        requester_pays: Optional[bool] = None,
        cache_type: Optional[str] = None,
        cache_options: Optional[Mapping[str, Any]] = None,
        compression: Optional[str] = None, 
        autocommit: Optional[bool] = True,
        **kwargs: Any,
    ) -> IterableAIOFile:
        return get_cloud_file(
            self.open(
                mode=mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                # acl=acl,
                version_id=version_id,
                fill_cache=fill_cache,
                block_size=block_size,
                requester_pays=requester_pays,
                cache_type=cache_type,
                cache_options=cache_options,
                compression=compression,
                autocommit=autocommit,
                **kwargs,

            )
        )


                   
    # def async_open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
    #     """
    #     Asyncronously Open the file pointed by this path and return a file object, as
    #     the built-in open() function does.
    #     compression = infer doesn't work all that well.
    #     """
    #     #self._fileio = self._accessor.open(self._cloudpath, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, buffering=buffering, **kwargs)
    #     #print(type(self._fileio))
    #     #return get_cloud_file(self._fileio)
    #     return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, buffering=buffering, **kwargs))

    
    # Implement some stuff that boto is faster in
    def upload_file(self, dest: PathLike, filename: Optional[str] = None, overwrite: bool = True, **kwargs):
        """
        Upload a file to R2

        Utilize boto3
        """
        if not overwrite and dest.exists(): raise FileExistsError(f"{dest} already exists and overwrite is False")
        filename = filename or self.name
        self._accessor.boto.upload_file(
            Bucket = self._bucket,
            Key = self.get_path_key(filename),
            Filename = dest.as_posix(),
        )
        return self.parent.joinpath(filename)
    
    def download_file(
        self, 
        output_file: Optional[PathLike] = None,
        output_dir: Optional[PathLike] = None,
        filename: Optional[str] = None,
        overwrite: bool = True,
        callbacks: Optional[List[Any]] = None,
        **kwargs
        ):
        """
        Downloads a file from R2 to a path
        """
        assert output_file or output_dir, "Must provide either output_file or output_dir"
        output_file = output_file or output_dir.joinpath(filename or self.name)
        assert overwrite or not output_file.exists(), f"{output_file} already exists and overwrite is False"
        s3t = self._accessor.s3t()
        s3t.download(
            self._bucket,
            self.get_path_key(self.name),
            output_file.as_posix(),
            subscribers = callbacks
        )
        s3t.shutdown()
        return output_file

    async def async_upload_file(self, dest: PathLike, filename: Optional[str] = None,  overwrite: bool = True, **kwargs):
        """
        Upload a file to R2

        Utilize boto3
        """
        if not overwrite and await dest.async_exists(): raise FileExistsError(f"{dest} already exists and overwrite is False")
        filename = filename or self.name
        s3t = self._accessor.s3t()
        s3t.upload(
            dest.as_posix(),
            self._bucket,
            self.get_path_key(filename)
        )
        s3t.shutdown()
        #await to_thread(
        #    self._accessor.boto.upload_file, Bucket = self._bucket, Key = self.get_path_key(filename), Filename = dest.as_posix()
        #)
        return self.parent.joinpath(filename)

    def batch_upload_files(
        self, 
        files: Optional[List[PathLike]] = None,
        glob_path: Optional[str] = None,
        overwrite: bool = False,
        skip_existing: bool = True,
        callbacks: Optional[List[Any]] = None,
        **kwargs
    ):
        """
        Handles batch uploading of files

        https://stackoverflow.com/questions/56639630/how-can-i-increase-my-aws-s3-upload-speed-when-using-boto3
        """
        assert files or glob_path, "Must provide either files or glob_path"
        if glob_path: files = list(self.glob(glob_path))
        results = []
        s3t = self._accessor.s3t()
        for file in files:
            if not overwrite and skip_existing and file.exists(): continue
            s3t.upload(
                file.as_posix(),
                self._bucket,
                self.get_path_key(file.name),
                subscribers = callbacks
            )
            results.append(self.parent.joinpath(file.name))
        s3t.shutdown()
        return results
    
    def batch_download_files(
        self,
        glob_path: str,
        output_dir: PathLike,
        overwrite: bool = False,
        skip_existing: bool = True,
        callbacks: Optional[List[Any]] = None,
        **kwargs
    ):
        """
        Handles batch downloading of files
        """
        files = list(self.glob(glob_path))
        results = []
        s3t = self._accessor.s3t()
        for file in files:
            if not overwrite and skip_existing and file.exists(): continue
            output_file = output_dir.joinpath(file.name)
            s3t.download(
                self._bucket,
                self.get_path_key(file.name),
                output_file.as_posix(),
                subscribers = callbacks
            )
            results.append(output_file)
        s3t.shutdown()
        return results



class FileR2PosixPath(PosixPath, FileR2Path, PureFileR2PosixPath):
    __slots__ = ()


class FileR2WindowsPath(WindowsPath, FileR2Path, PureFileR2WindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("FileR2Path.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("FileR2Path.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        FileR2PurePath, FileR2Path, PureFileR2PosixPath, FileR2WindowsPath, FileR2PosixPath, PureFileR2WindowsPath
    ]
)

R2FileSystem = R2_CloudFileSystem


__all__ = (
    'FileR2PurePath',
    'FileR2Path',
    'PureFileR2PosixPath',
    'FileR2WindowsPath',
    'FileR2PosixPath',
    'PureFileR2WindowsPath',
    'R2FileSystem'
)
