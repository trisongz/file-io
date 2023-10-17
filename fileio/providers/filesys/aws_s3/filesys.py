import os
import warnings
from .base import *
from .utils import _log

with contextlib.suppress(ImportError):
    import s3fs
    from s3fs.errors import translate_boto_error
    from botocore.exceptions import ClientError
    from s3fs.core import _error_wrapper, sync_wrapper


if TYPE_CHECKING:
    from s3transfer.manager import TransferManager
    from aiobotocore.session import AioSession

class S3FileSystem(s3fs.S3FileSystem):
    """
    Access S3 as if it were a file system.

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

    root_marker = ""
    connect_timeout = 5
    retries = 5
    read_timeout = 15
    default_block_size = 5 * 2**20
    protocol = ["s3", "s3a"]
    _extra_tokenize_attributes = ("default_block_size",)

    s3tm: Optional['TransferManager'] = None

    def __init_dircache(self, **storage_options):
        """
        Initialize the directory cache
        """
        # self.dircache = DirCache(**storage_options)


    def __init_fs(self, *args, **storage_options):
        """Create and configure file-system instance

        Instances may be cachable, so if similar enough arguments are seen
        a new instance is not required. The token attribute exists to allow
        implementations to cache instances if they wish.

        A reasonable default should be provided if there are no arguments.

        Subclasses should call this method.

        Parameters
        ----------
        use_listings_cache, listings_expiry_time, max_paths:
            passed to ``DirCache``, if the implementation supports
            directory listing caching. Pass use_listings_cache=False
            to disable such caching.
        skip_instance_cache: bool
            If this is a cachable implementation, pass True here to force
            creating a new instance even if a matching instance exists, and prevent
            storing this instance.
        asynchronous: bool
        loop: asyncio-compatible IOLoop or None
        """
        # reusing instance, don't change
        if self._cached: return
        self._cached = True
        self._intrans = False
        self._transaction = None
        self._invalidated_caches_in_transaction = []
        self.__init_dircache(**storage_options)
        # self.dircache = DirCache(**storage_options)

        if storage_options.pop("add_docs", None):
            warnings.warn("add_docs is no longer supported.", FutureWarning)

        if storage_options.pop("add_aliases", None):
            warnings.warn("add_aliases has been removed.", FutureWarning)
        
        # This is set in _Cached
        self._fs_token_ = None

    def __init_afs(self, *args, asynchronous: Optional[bool] = False, loop: Optional[asyncio.AbstractEventLoop] = None, batch_size: Optional[int] = None, **kwargs):
        """
        The asynchronous version of __init_fs
        """
        self.asynchronous = asynchronous
        self._pid = os.getpid()
        self._loop = None if asynchronous else loop or get_loop()
        self.batch_size = batch_size
        self.__init_fs(*args, **kwargs)


    def __init__(
        self,
        anon: Optional[bool] = False,
        endpoint_url: Optional[str] = None,
        key: Optional[str] = None,
        secret: Optional[str] = None,
        token: Optional[str] = None,
        use_ssl: Optional[bool] = True,
        client_kwargs: Optional[Dict[str, Any]] = None,
        requester_pays: Optional[bool] = False,
        default_block_size: Optional[int] =None,
        default_fill_cache: Optional[bool] = True,
        default_cache_type: Optional[str] = "readahead",
        version_aware: Optional[bool] = False,
        config_kwargs: Optional[Dict[str, Any]] = None,
        s3_additional_kwargs: Optional[Dict[str, Any]]= None,
        session: Optional['AioSession'] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        cache_regions: Optional[bool] = False,
        asynchronous: Optional[bool] = False,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs,
    ):
        if key and username:
            raise KeyError("Supply either key or username, not both")
        if secret and password:
            raise KeyError("Supply secret or password, not both")
        if username:
            key = username
        if password:
            secret = password

        self.endpoint_url = endpoint_url

        self.anon = anon
        self.key = key
        self.secret = secret
        self.token = token
        self.kwargs = kwargs
        super_kwargs = {
            k: kwargs.pop(k)
            for k in ["use_listings_cache", "listings_expiry_time", "max_paths"]
            if k in kwargs
        }  # passed to fsspec superclass
        self.__init_afs(loop = loop, asynchronous = asynchronous, **super_kwargs)
        # super().__init__(loop=loop, asynchronous=asynchronous, **super_kwargs)

        self.default_block_size = default_block_size or self.default_block_size
        self.default_fill_cache = default_fill_cache
        self.default_cache_type = default_cache_type
        self.version_aware = version_aware
        self.client_kwargs = client_kwargs or {}
        self.config_kwargs = config_kwargs or {}
        self.req_kw = {"RequestPayer": "requester"} if requester_pays else {}
        self.s3_additional_kwargs = s3_additional_kwargs or {}
        self.use_ssl = use_ssl
        self.cache_regions = cache_regions
        self._s3 = None
        self.session = session


    def _get_block_size(
        self,
        mode: str = "rb",
        block_size: Optional[int] = None,
    ) -> int:
        """
        Get the block size for reading or writing a file
        """
        if block_size is None:  return self.default_r2_write_block_size if 'w' in mode else self.default_r2_block_size
        return max(block_size, (self.default_r2_write_block_size if 'w' in mode else self.default_r2_block_size))


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

        block_size = self._get_block_size(mode, block_size)
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

    async def open_async(self, path, mode="rb", block_size: Optional[int] = None, **kwargs):
        if "b" not in mode: # or kwargs.get("compression"):
            raise ValueError
        _ = kwargs.pop("compression", None)

        block_size = self._get_block_size(mode, block_size)
        return R2AsyncStreamedFile(self, path, mode, block_size = block_size, **kwargs)
