"""
Subclass for Cloudflare R2
"""

import io
import re
import contextlib
from fileio.utils import logger
from typing import Optional, Dict, Any, Union, List, TYPE_CHECKING

with contextlib.suppress(ImportError):
    import s3fs
    from s3fs.errors import translate_boto_error
    from botocore.exceptions import ClientError
    from s3fs.core import _error_wrapper, sync_wrapper

if TYPE_CHECKING:
    from s3transfer.manager import TransferManager


bucket_format_list = [
    re.compile(
        r"^(?P<bucket>arn:(aws).*:r2:[a-z\-0-9]*:[0-9]{12}:accesspoint[:/][^/]+)/?"
        r"(?P<key>.*)$"
    ),
    re.compile(
        r"^(?P<bucket>arn:(aws).*:s3:[a-z\-0-9]*:[0-9]{12}:accesspoint[:/][^/]+)/?"
        r"(?P<key>.*)$"
    ),
    re.compile(
        r"^(?P<bucket>arn:(aws).*:s3-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
        r"[a-zA-Z0-9\-]{1,63}[/:](bucket|accesspoint)[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
    ),
    re.compile(
        r"^(?P<bucket>arn:(aws).*:s3-outposts:[a-z\-0-9]+:[0-9]{12}:outpost[/:]"
        r"[a-zA-Z0-9\-]{1,63}[/:]bucket[/:]"
        r"[a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
    ),
    re.compile(
        r"^(?P<bucket>arn:(aws).*:s3-object-lambda:[a-z\-0-9]+:[0-9]{12}:"
        r"accesspoint[/:][a-zA-Z0-9\-]{1,63})[/:]?(?P<key>.*)$"
    ),
]

# MPU seems to work > 150MB?
_debug_mode: bool = True

def _log(f, *args, **kwargs):
    if _debug_mode:
        logger.debug(f, *args, **kwargs)
    else:
        logger.info(f, *args, **kwargs)

class R2FileSystem(s3fs.S3FileSystem):
    """
    Access R2 as if it were a file system.

    This exposes a filesystem-like API (ls, cp, open, etc.) on top of S3
    storage.

    Provide credentials either explicitly (``key=``, ``secret=``) or depend
    on boto's credential methods. See botocore documentation for more
    information. If no credentials are available, use ``anon=True``.

    Parameters
    ----------
    anon : bool (False)
        Whether to use anonymous connection (public buckets only). If False,
        uses the key/secret given, or boto's credential resolver (client_kwargs,
        environment, variables, config files, EC2 IAM server, in that order)
    endpoint_url : string (None)
        Use this endpoint_url, if specified. Needed for connecting to non-AWS
        S3 buckets. Takes precedence over `endpoint_url` in client_kwargs.
    key : string (None)
        If not anonymous, use this access key ID, if specified. Takes precedence
        over `aws_access_key_id` in client_kwargs.
    secret : string (None)
        If not anonymous, use this secret access key, if specified. Takes
        precedence over `aws_secret_access_key` in client_kwargs.
    token : string (None)
        If not anonymous, use this security token, if specified
    use_ssl : bool (True)
        Whether to use SSL in connections to S3; may be faster without, but
        insecure. If ``use_ssl`` is also set in ``client_kwargs``,
        the value set in ``client_kwargs`` will take priority.
    s3_additional_kwargs : dict of parameters that are used when calling s3 api
        methods. Typically used for things like "ServerSideEncryption".
    client_kwargs : dict of parameters for the botocore client
    requester_pays : bool (False)
        If RequesterPays buckets are supported.
    default_block_size: int (None)
        If given, the default block size value used for ``open()``, if no
        specific value is given at all time. The built-in default is 5MB.
    default_fill_cache : Bool (True)
        Whether to use cache filling with open by default. Refer to
        ``S3File.open``.
    default_cache_type : string ("readahead")
        If given, the default cache_type value used for ``open()``. Set to "none"
        if no caching is desired. See fsspec's documentation for other available
        cache_type values. Default cache_type is "readahead".
    version_aware : bool (False)
        Whether to support bucket versioning.  If enable this will require the
        user to have the necessary IAM permissions for dealing with versioned
        objects. Note that in the event that you only need to work with the
        latest version of objects in a versioned bucket, and do not need the
        VersionId for those objects, you should set ``version_aware`` to False
        for performance reasons. When set to True, filesystem instances will
        use the S3 ListObjectVersions API call to list directory contents,
        which requires listing all historical object versions.
    cache_regions : bool (False)
        Whether to cache bucket regions or not. Whenever a new bucket is used,
        it will first find out which region it belongs and then use the client
        for that region.
    asynchronous :  bool (False)
        Whether this instance is to be used from inside coroutines.
    config_kwargs : dict of parameters passed to ``botocore.client.Config``
    kwargs : other parameters for core session.
    session : aiobotocore AioSession object to be used for all connections.
         This session will be used inplace of creating a new session inside S3FileSystem.
         For example: aiobotocore.session.AioSession(profile='test_user')

    The following parameters are passed on to fsspec:

    skip_instance_cache: to control reuse of instances
    use_listings_cache, listings_expiry_time, max_paths: to control reuse of directory listings

    Examples
    --------
    >>> s3 = S3FileSystem(anon=False)  # doctest: +SKIP
    >>> s3.ls('my-bucket/')  # doctest: +SKIP
    ['my-file.txt']

    >>> with s3.open('my-bucket/my-file.txt', mode='rb') as f:  # doctest: +SKIP
    ...     print(f.read())  # doctest: +SKIP
    b'Hello, world!'
    """
    # Since there seems to be a problem with the s3fs implementation of
    # multipart uploads, we are going to just use the max block size
    # default_r2_block_size = 10 * 1024 * 1024
    default_r2_block_size: int = 150 * 1024 * 1024 # 150MB
    default_r2_max_size: int = 5 * 1024 * 1024 * 1024 # 5GB

    s3tm: Optional['TransferManager'] = None


    def _find_bucket_key(self, s3_path: str):
        """
        This is a helper function that given an s3 path such that the path is of
        the form: bucket/key
        It will return the bucket and the key represented by the s3 path
        """
        # if s3_path.startswith("r2://"):
        #     s3_path = s3_path.replace('r2://', 's3://')
        
        for bucket_format in bucket_format_list:
            match = bucket_format.match(s3_path)
            if match:
                return match.group("bucket"), match.group("key")
        s3_components = s3_path.split("/", 1)
        bucket = s3_components[0]
        s3_key = s3_components[1] if len(s3_components) > 1 else ""
        return bucket, s3_key

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        acl: Optional[str] = "",
        version_id: Optional[str] = None,
        fill_cache: Optional[bool] = None,
        cache_type: Optional[str] = None,
        autocommit: Optional[bool] = True,
        requester_pays: Optional[bool] = None,
        cache_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> 'R2File':
        """Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on S3
        mode: string
            One of 'r', 'w', 'a', 'rb', 'wb', or 'ab'. These have the same meaning
            as they do for the built-in `open` function.
        block_size: int
            Size of data-node blocks if reading
        fill_cache: bool
            If seeking to new a part of the file beyond the current buffer,
            with this True, the buffer will be filled between the sections to
            best support random access. When reading only a few specific chunks
            out of a file, performance may be better if False.
        acl: str
            Canned ACL to set when writing
        version_id : str
            Explicit version of the object to open.  This requires that the s3
            filesystem is version aware and bucket versioning is enabled on the
            relevant bucket.
        encoding : str
            The encoding to use if opening the file in text mode. The platform's
            default text encoding is used if not given.
        cache_type : str
            See fsspec's documentation for available cache_type values. Set to "none"
            if no caching is desired. If None, defaults to ``self.default_cache_type``.
        requester_pays : bool (optional)
            If RequesterPays buckets are supported.  If None, defaults to the
            value used when creating the S3FileSystem (which defaults to False.)
        kwargs: dict-like
            Additional parameters used for s3 methods.  Typically used for
            ServerSideEncryption.
        """

        if block_size is None: block_size = self.default_r2_block_size
        else: block_size = max(block_size, self.default_r2_block_size)
        if fill_cache is None: fill_cache = self.default_fill_cache
        if requester_pays is None: requester_pays = bool(self.req_kw)
        acl = acl or self.s3_additional_kwargs.get("ACL", "")
        kw = self.s3_additional_kwargs.copy()
        kw.update(kwargs)
        if not self.version_aware and version_id:
            raise ValueError(
                "version_id cannot be specified if the filesystem "
                "is not version aware"
            )

        if cache_type is None: cache_type = self.default_cache_type
        # _log(f's3tm: {self.s3tm}')
        return R2File(
            self,
            path,
            mode,
            block_size=block_size,
            acl=acl,
            version_id=version_id,
            fill_cache=fill_cache,
            s3_additional_kwargs=kw,
            cache_type=cache_type,
            autocommit=autocommit,
            requester_pays=requester_pays,
            cache_options=cache_options,
        )

    
    async def _rm_file(self, path, **kwargs):
        bucket, key, _ = self.split_path(path)
        delete_keys = {
            "Objects": [{"Key": key}],
            "Quiet": True,
        }
        self.invalidate_cache(self._parent(path))
        # logger.info(f'Deleting {path}: {delete_keys}')
        try:
            # await self._call_s3("delete_object", Bucket=bucket, Key=key)
            await self._call_s3("delete_objects", Bucket=bucket, Delete = delete_keys)
        except ClientError as e:
            raise translate_boto_error(e) from e
        
    
    async def _call_s3(self, method, *akwarglist, **kwargs):
        await self.set_session()
        s3 = await self.get_s3(kwargs.get("Bucket"))
        method = getattr(s3, method)
        kw2 = kwargs.copy()
        kw2.pop("Body", None)
        _log(f"CALL: {method.__name__} - {akwarglist} - {kw2}")
        additional_kwargs = self._get_s3_method_kwargs(method, *akwarglist, **kwargs)
        return await _error_wrapper(
            method, kwargs=additional_kwargs, retries=self.retries
        )
    
    call_s3 = sync_wrapper(_call_s3)

class R2File(s3fs.S3File):
    fs: 'R2FileSystem' = None

    def _call_s3(self, method, *kwarglist, **kwargs):
        """
        Filter out ACL for methods that we know will fail
        """
        # if method in ["create_multipart_upload", "put_object", "put_object_acl"]:
        acl = kwargs.pop("ACL", None)
        # logger.debug(f"ACL: {acl}")
        _log(f'calling method: {method}')
        return self.fs.call_s3(method, self.s3_additional_kwargs, *kwarglist, **kwargs)


    def _upload_chunk(self, final=False):
        bucket, key, _ = self.fs.split_path(self.path)
        _log(
            f"Upload for {self}, final={final}, loc={self.loc}, buffer loc={self.buffer.tell()}"
        )
        if (
            self.autocommit
            and not self.append_block
            and final
            and self.tell() < self.blocksize
        ):
            # only happens when closing small file, use on-shot PUT
            data1 = False
        else:
            self.buffer.seek(0)
            (data0, data1) = (None, self.buffer.read(self.blocksize))

        while data1:
            (data0, data1) = (data1, self.buffer.read(self.blocksize))
            data1_size = len(data1)

            if 0 < data1_size < self.blocksize:
                remainder = data0 + data1
                remainder_size = self.blocksize + data1_size

                if remainder_size <= self.part_max:
                    (data0, data1) = (remainder, None)
                else:
                    partition = remainder_size // 2
                    (data0, data1) = (remainder[:partition], remainder[partition:])

            part = len(self.parts) + 1
            _log(f"Upload chunk {self}, {part}, {self.blocksize}/{data1_size}")

            out = self._call_s3(
                "upload_part",
                Bucket=bucket,
                PartNumber=part,
                UploadId=self.mpu["UploadId"],
                Body=data0,
                Key=key,
            )

            self.parts.append({"PartNumber": part, "ETag": out["ETag"]})

        if self.autocommit and final:
            self.commit()
        return not final
    

    def commit(self):
        _log(f"Commit {self}")
        if self.tell() == 0:
            if self.buffer is not None:
                _log(f"Empty file committed {self}")
                self._abort_mpu()
                write_result = self.fs.touch(self.path)
        elif not self.parts:
            if self.buffer is None:
                raise RuntimeError

            _log(f"One-shot upload of {self}: {self.key}")
            self.buffer.seek(0)
            data = self.buffer.read()
            write_result = self._call_s3(
                "put_object",
                Key=self.key,
                Bucket=self.bucket,
                Body=data,
                ACL=self.acl,
                **self.kwargs,
            )
        else:
            part_info = {"Parts": self.parts}
            _log(f"Complete multi-part upload for {self}: {self.key} {part_info}")
            try:
                write_result = self._call_s3(
                    "complete_multipart_upload",
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=self.mpu["UploadId"],
                    # Action="mpu-complete",
                    # Parts=part_info["Parts"],
                    MultipartUpload=part_info,

                )
                # self.mpu.complete()
            except Exception as e:
                self._abort_mpu()
                raise e

        if self.fs.version_aware: self.version_id = write_result.get("VersionId")
        # complex cache invalidation, since file's appearance can cause several
        # directories
        self.buffer = None
        parts = self.path.split("/")
        path = parts[0]
        for p in parts[1:]:
            if path in self.fs.dircache and not [
                True for f in self.fs.dircache[path] if f["name"] == path + "/" + p
            ]:
                self.fs.invalidate_cache(path)
            path = path + "/" + p

    def write(self, data: Union[bytes, bytearray, memoryview]):
        """
        Write data to buffer.

        Buffer only sent on flush() or if buffer is greater than
        or equal to blocksize.

        Parameters
        ----------
        data: bytes
            Set of bytes to be written.
        """
        if self.mode not in {"wb", "ab"}:
            raise ValueError("File not in write mode")
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self.forced:
            raise ValueError("This file has been force-flushed, can only close")
        out = self.buffer.write(data)
        self.loc += out
        if self.buffer.tell() >= self.blocksize:
            self.flush()
        return out

    def _large_upload(self, callbacks: Optional[List[Any]] = None, **kwargs):
        """
        Handles large file uploads bc multipart upload isnt working.
        """
        bucket, key, _ = self.fs.split_path(self.path)
        _log(f"Large upload for {self}, loc={self.loc}, buffer loc={self.buffer.tell()}")
        self.buffer.seek(0)
        self.fs.s3tm.upload(
            self.buffer,
            bucket,
            key,
            subscribers = callbacks
        )
        _log(f"Large upload for {self} complete")
        self.buffer = io.BytesIO()


    def flush(self, force: bool = False):
        """
        Write buffered data to backend store.

        Writes the current buffer, if it is larger than the block-size, or if
        the file is being closed.

        Parameters
        ----------
        force: bool
            When closing, write the last block even if it is smaller than
            blocks are allowed to be. Disallows further writing to this file.
        """

        if self.closed:
            raise ValueError("Flush on closed file")
        if force and self.forced:
            raise ValueError("Force flush cannot be called more than once")
        if force:
            self.forced = True

        if self.mode not in {"wb", "ab"}:
            # no-op to flush on read-mode
            return

        if not force and self.buffer.tell() < self.blocksize:
            # Defer write on small block
            return
        
        # if self.buffer.tell() >= self.fs.default_r2_max_size:
        # if self.buffer.tell() >= self.fs.default_r2_block_size:
        #     # Write large file
        #     self._large_upload()
        #     return

        if self.offset is None:
            # Initialize a multipart upload
            self.offset = 0
            try:
                self._initiate_upload()
            except:  # noqa: E722
                self.closed = True
                raise

        if self._upload_chunk(final=force) is not False:
            self.offset += self.buffer.seek(0, 2)
            self.buffer = io.BytesIO()