import datetime
import contextlib
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING
from pydantic import BaseModel, Field
from .classprops import lazyproperty


if TYPE_CHECKING:
    from fileio.core.types import FileLike
    with contextlib.suppress(ImportError):
        from starlette.requests import Request
        from starlette.datastructures import UploadFile

class FileInfo(BaseModel):
    size: Optional[int] = None
    etag: Optional[str] = None
    last_modified: Optional[datetime.datetime] = None
    checksum: Optional[str] = None
    path: Optional['FileLike'] = None

    class Config:
        allow_arbitrary_types = True

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        if d['path']: d['path'] = self.path.as_posix()
        return d

    def validate_info(self):
        """
        Ensures that the file info is valid.
        """
        if not self.path or not self.path.exists():
            raise ValueError('Path does not exist.')
        if self.checksum is None:
            from fileio.utils.ops import checksum_file
            self.checksum = checksum_file(self.path)
        if not all(self.size, self.etag, self.last_modified):
            info: Dict[str, Any] = self.path.info()
            self.size = info.get('size', info.get('Size', 0))
            self.etag = info.get('ETag') or 'none'
            self.last_modified = info.get('LastModified')
        
    async def async_validate_info(self):
        """
        Ensures that the file info is valid.
        """
        if not self.path or not await self.path.async_exists():
            raise ValueError('Path does not exist.')
        if self.checksum is None:
            from fileio.utils.ops import async_checksum_file
            self.checksum = await async_checksum_file(self.path)
        if not all(self.size, self.etag, self.last_modified):
            info: Dict[str, Any] = await self.path.async_info()
            self.size = info.get('size', info.get('Size', 0))
            self.etag = info.get('ETag') or 'none'
            self.last_modified = info.get('LastModified')

    @classmethod
    def get_info(cls, path: 'FileLike') -> 'FileInfo':
        """
        Fetches file info from the given path.
        """
        from fileio import File
        from fileio.utils.ops import checksum_file

        path = File(path)
        file_info: Dict[str, Any] = path.info()
        return cls(
            size = file_info.get('size', file_info.get('Size', 0)),
            etag = file_info.get('ETag') or 'none',
            last_modified = file_info.get('LastModified'),
            checksum = checksum_file(path = path),
            path = path
        )
    
    @classmethod
    async def async_get_info(cls, path: 'FileLike') -> 'FileInfo':
        """
        Fetches file info from the given path.
        """
        from fileio import File
        from fileio.utils.ops import async_checksum_file
        
        path = File(path)
        file_info: Dict[str, Any] = await path.async_info()
        return cls(
            size = file_info.get('size', file_info.get('Size', 0)),
            etag = file_info.get('ETag') or 'none',
            last_modified = file_info.get('LastModified'),
            checksum = await async_checksum_file(path = path),
            path = path
        )

    def __eq__(self, other):
        if isinstance(other, FileInfo):
            return self.checksum == other.checksum
        return False



class PreparedFile(BaseModel):

    local_path: Optional[Union[Any, 'FileLike']] = None
    remote_path: Optional[Union[Any, 'FileLike']] = None
    is_tmp: Optional[bool] = None
    checksum: Optional[str] = None

    @lazyproperty
    def is_temp(self):
        return self.is_tmp or (self.local_path is not None and 'tmp' in self.local_path.as_posix())
    
    @lazyproperty
    def source_path(self) -> 'FileLike':
        return self.local_path or self.remote_path
    
    @lazyproperty
    def file_info(self) -> FileInfo:
        return FileInfo.get_info(self.source_path)
    
    @classmethod
    def from_path(cls, path: 'FileLike', **kwargs):
        from fileio import File
        from fileio.utils.ops import checksum_file
        p = File(path)
        cksum = checksum_file(p)
        kws = {
            'local_path': None if p.is_cloud else p,
            'remote_path': p if p.is_cloud else None,
            'is_tmp': not p.is_cloud,
            'checksum': cksum,
            **kwargs,
        }
        return cls.parse_obj(kws)
    
    @classmethod
    async def async_from_path(cls, path: 'FileLike', **kwargs):
        from fileio import File
        from fileio.utils.ops import async_checksum_file
        p = File(path)
        cksum = await async_checksum_file(p)
        kws = {
            'local_path': None if p.is_cloud else p,
            'remote_path': p if p.is_cloud else None,
            'is_tmp': not p.is_cloud,
            'checksum': cksum,
            **kwargs,
        }
        return cls.parse_obj(kws)
    
    @classmethod
    def from_url(cls, url, **kwargs):
        from fileio.utils.ops import checksum_file, fetch_file_from_url
        p = fetch_file_from_url(url, **kwargs)
        cksum = checksum_file(p)
        kws = {
            'local_path': None if p.is_cloud else p,
            'remote_path': p if p.is_cloud else None,
            'is_tmp': not p.is_cloud,
            'checksum': cksum,
            **kwargs,
        }
        return cls.parse_obj(kws)
    
    @classmethod
    async def async_from_url(cls, url, **kwargs):
        from fileio.utils.ops import async_checksum_file, async_fetch_file_from_url
        p = await async_fetch_file_from_url(url, **kwargs)
        cksum = await async_checksum_file(p)
        kws = {
            'local_path': None if p.is_cloud else p,
            'remote_path': p if p.is_cloud else None,
            'is_tmp': not p.is_cloud,
            'checksum': cksum,
            **kwargs,
        }
        return cls.parse_obj(kws)

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        if d['local_path']: d['local_path'] = self.local_path.as_posix()
        if d['remote_path']: d['remote_path'] = self.remote_path.as_posix()
        d['source_path'] = self.source_path.as_posix()
        d['is_tmp'] = d.pop('is_temp', False)
        return d

    @classmethod
    async def async_from_remote(
        cls,
        request: Optional['Request'] = None, 
        remote_files: Optional[Union[List[str], str]] = None,
        remote_form_keys: Optional[List[str]] = None,
        **kwargs
    ) -> Union['PreparedFile', List['PreparedFile']]:
        """
        Create a PreparedFile from a remote file(s)
        """
        if not remote_files and request:
            form_data = await request.form()
            remote_files: List[str] = []
            if not remote_form_keys:
                remote_form_keys = ['url', 'urls', 'remote', 'remote_file', 'remote_files']
                for k in remote_form_keys:
                    if form_data.get(k):
                        if isinstance(form_data[k], list):
                            remote_files.extend(form_data[k])
                        else:
                            remote_files.append(form_data[k])

        if not remote_files: raise ValueError("No remote file(s) found")
        if not isinstance(remote_files, list): remote_files = [remote_files]
        
        
        from fileio import File
        from fileio.utils.ops import async_checksum_file, async_fetch_file_from_url
        files = []
        for f in remote_files:
            if 'http' in f: remote_file = await async_fetch_file_from_url(url = f, **kwargs)
            else: remote_file = File(f)
            cksum = await async_checksum_file(remote_file)
            kws = {
                'local_path': None if remote_file.is_cloud else remote_file,
                'remote_path': remote_file if remote_file.is_cloud else None,
                'is_tmp': not remote_file.is_cloud,
                'checksum': cksum,
                **kwargs,
            }
            files.append(cls.parse_obj(kws))
        return files if len(files) > 1 else files[0]
    
    @classmethod
    async def async_from_upload(
        cls, 
        request: Optional['Request'] = None, 
        upload_files: Optional[Union[List['UploadFile'], 'UploadFile']] = None, 
        upload_form_keys: Optional[Union[List[str], str]] = None,
        **kwargs
    ) -> Union['PreparedFile', List['PreparedFile']]:
        """
        Create a PreparedFile from a request or upload_file
        """
        if not upload_files and request:
            form_data = await request.form()
            upload_files: List['UploadFile'] = []
            if not upload_form_keys:
                upload_form_keys = ['file', 'files', 'upload', 'upload_file', 'upload_files']
                for k in upload_form_keys:
                    if form_data.get(k):
                        if isinstance(form_data[k], list):
                            upload_files.extend(form_data[k])
                        else:
                            upload_files.append(form_data[k])

        if not upload_files: raise ValueError("No upload file(s) found")
        if not isinstance(upload_files, list): upload_files = [upload_files]

        from fileio import File
        from fileio.utils.ops import async_checksum_file
        from starlette.datastructures import UploadFile

        files = []
        for f in upload_files:
            _is_tmp = False
            if isinstance(f, UploadFile):
                file: FileLike = File.get_tempfile()
                await file.async_write_bytes(await f.read())
                _is_tmp = True
            else: file = File(f)
            cksum = await async_checksum_file(file)
            kws = {
                'local_path': None if file.is_cloud else file,
                'remote_path': file if file.is_cloud else None,
                'is_tmp': _is_tmp or not file.is_cloud,
                'checksum': cksum,
                **kwargs,
            }
            files.append(cls.parse_obj(kws))
        return files if len(files) > 1 else files[0]


    @classmethod
    async def async_from_request(
        cls, 
        request: Optional['Request'] = None, 
        upload_files: Optional[Union[List['UploadFile'], 'UploadFile']] = None, 
        upload_form_keys: Optional[Union[List[str], str]] = None,
        remote_files: Optional[Union[List[str], str]] = None,
        remote_form_keys: Optional[List[str]] = None,
        **kwargs
    ) -> Union['PreparedFile', List['PreparedFile']]:
        """
        Create a PreparedFile from a request or upload_file
        """
        if remote_files or remote_form_keys:
            return await cls.async_from_remote(request = request, remote_files = remote_files, remote_form_keys = remote_form_keys, **kwargs)
        return await cls.async_from_upload(request = request, upload_files = upload_files, upload_form_keys = upload_form_keys, **kwargs)

    class Config:
        allow_arbitrary_types = True
        extra = 'allow'


class ParsedFile(BaseModel):
    content: Union[str, Dict[str, Any], Any]
    metadata: Optional[Dict[str, Any]] = {}
    file: Optional[PreparedFile] = Field(default_factory = PreparedFile)

    @lazyproperty
    def file_info(self) -> FileInfo:
        return self.file.file_info

    def dict(self, *args, **kwargs):
        d = super().dict(*args, **kwargs)
        if d['file']: d['file'] = self.file.dict()
        return d

    class Config:
        allow_arbitrary_types = True
        extra = 'allow'