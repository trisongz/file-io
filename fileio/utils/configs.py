import os
import json
import pathlib
import multiprocessing as mp

from typing import Optional, Dict, Any, Union
from fileio.utils.logs import default_logger as logger
from fileio.types.classprops import lazyproperty
from fileio.types.compat import BaseSettings, validator, root_validator

class CoreSettings(BaseSettings):

    authz_config_dir: Optional[pathlib.Path] = None
    boto_config: Optional[pathlib.Path] = None
    max_workers: Optional[int] = None

    class Config:
        env_prefix: str = ""

    @validator("authz_config_dir", pre=True)
    def validate_authz_config_dir(cls, v):
        return pathlib.Path("~/.auth").expanduser() if v is None else v
    
    @lazyproperty
    def num_workers(self):
        return self.max_workers or min(2, round(mp.cpu_count() // 2))

    @lazyproperty
    def boto_config_path(self) -> pathlib.Path:
        if self.boto_config is None:
            return pathlib.Path('/root/.boto') if self.in_colab else pathlib.Path("~/.boto").expanduser()
        return self.boto_config

    @lazyproperty
    def boto_config_exists(self):
        return self.boto_config_path.exists()

    @lazyproperty
    def user_home(self) -> pathlib.Path:
        if self.in_colab:
            return pathlib.Path("/content")
        return pathlib.Path("~").expanduser()
    
    @lazyproperty
    def in_colab(self) -> bool:
        """
        Checks if the code is running in Google Colab
        """
        from fileio.utils.lazylib import LazyLib
        return LazyLib.is_available('google.colab')

    
    def set_env(self):
        """
        Sets the environment variables for boto
        """
        if self.boto_config_exists:
            os.environ["BOTO_CONFIG"] = self.boto_config_path.as_posix()
            os.environ["BOTO_PATH"] = self.boto_config_path.as_posix()
    
    def update_config(self, **kwargs):
        """
        Updates the settings
        """
        for k, v in kwargs.items():
            if not hasattr(self, k):  continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)


core_settings = CoreSettings()

class AwsSettings(BaseSettings):
    aws_access_token: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: Optional[str] = "us-east-1"
    set_s3_endpoint: Optional[bool] = False
    s3_config: Optional[Union[str, Dict[str, Any]]] = None

    class Config:
        env_prefix: str = ""

    @validator("s3_config", pre=True)
    def validate_s3_config(cls, v):
        if v is None: return {}
        return json.loads(v) if isinstance(v, str) else v
    
    @lazyproperty
    def s3_endpoint(self):
        return f'https://s3.{self.aws_region}.amazonaws.com'
    
    def set_env(self):
        if self.aws_access_key_id:
            os.environ['AWS_ACCESS_KEY_ID'] = self.aws_access_key_id
        if self.aws_secret_access_key:
            os.environ['AWS_SECRET_ACCESS_KEY'] = self.aws_secret_access_key
        if self.aws_region:
            os.environ['AWS_REGION'] = self.aws_region
        if self.aws_access_token:
            os.environ['AWS_ACCESS_TOKEN'] = self.aws_access_token
        if self.set_s3_endpoint:
            os.environ['S3_ENDPOINT'] = self.s3_endpoint
    

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):  continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, update_fs: bool = True, **config):
        self.update_config(**config)
        self.set_env()

        if update_fs:
            from fileio.lib.posix.filesys import FileSysManager
            FileSysManager.get_accessor('s3', _reset=True)

    def build_s3fs_config(self) -> Dict[str, Any]:
        """
        Builds the s3fs config dict
        """
        config = {}
        if self.aws_access_key_id:
            config["key"] = self.aws_access_key_id
            # config['aws_access_key_id'] = self.aws_access_key_id
        if self.aws_secret_access_key:
            config["secret"] = self.aws_secret_access_key
            # config['aws_secret_access_key'] = self.aws_secret_access_key
        if self.aws_access_token:
            config["token"] = self.aws_access_token
        if not (config.get('key') and config.get('secret')) and not core_settings.boto_config_exists:
            config['anon'] = True
        if self.set_s3_endpoint:
            config['client_kwargs'] = {'endpoint_url': self.s3_endpoint, 'region_name': self.aws_region}
        if self.s3_config:
            config['config_kwargs'] = self.s3_config
        
        return config


class GcpSettings(BaseSettings):
    gcp_project: Optional[str] = None
    gcloud_project: Optional[str] = None
    google_cloud_project: Optional[str] = None
    google_application_credentials: Optional[pathlib.Path] = None

    gcs_client_config: Optional[Union[str, Dict[str, Any]]] = None
    gcs_config: Optional[Union[str, Dict[str, Any]]] = None

    class Config:
        env_prefix: str = ""

    @validator("google_application_credentials")
    def validate_google_application_credentials(cls, v):
        return core_settings.user_home.joinpath('adc.json') if v is None else v

    @validator("gcs_client_config")
    def validate_gcs_client_config(cls, v) -> Dict:
        if v is None: return {}
        return json.loads(v) if isinstance(v, str) else v
    
    @validator("gcs_config")
    def validate_gcs_config(cls, v) -> Dict:
        if v is None: return {}
        return json.loads(v) if isinstance(v, str) else v

    @lazyproperty
    def adc_exists(self):
        return self.google_application_credentials.exists()
    
    @lazyproperty
    def project(self):
        return self.gcp_project or self.gcloud_project or self.google_cloud_project
    
    def set_env(self):
        if self.adc_exists:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_application_credentials.as_posix()
        if self.project:
            os.environ["GOOGLE_CLOUD_PROJECT"] = self.project
    
    def build_gcsfs_config(self) -> Dict[str, Any]:
        """
        Builds the gcsfs config kwargs
        """
        config = {}
        if self.adc_exists: config['token'] = self.google_application_credentials.as_posix()
        if self.project: config['project'] = self.project
        if self.gcs_client_config: config['client_config'] = self.gcs_client_config
        if self.gcs_config: config['config_kwargs'] = self.gcs_config
        return config

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):  continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, update_fs: bool = True, **config):
        self.update_config(**config)
        self.set_env()

        if update_fs:
            # Reset the accessor to use the new settings
            from fileio.lib.posix.filesys import FileSysManager
            FileSysManager.get_accessor('gs', _reset=True)

    

class MinioSettings(BaseSettings):
    minio_endpoint: Optional[str] = None
    minio_access_key: Optional[str] = None
    minio_secret_key: Optional[str] = None
    minio_access_token: Optional[str] = None
    minio_secure: Optional[bool] = True
    minio_region: Optional[str] = None
    minio_config: Optional[Union[str, Dict[str, Any]]] = None
    minio_signature_ver: Optional[str] = 's3v4'

    class Config:
        env_prefix: str = ""

    @validator("minio_config", pre=True)
    def validate_minio_config(cls, v):
        if v is None: return {}
        return json.loads(v) if isinstance(v, str) else v
    
    def set_env(self):
        if self.minio_endpoint:
            os.environ["MINIO_ENDPOINT"] = self.minio_endpoint
        if self.minio_access_key:
            os.environ["MINIO_ACCESS_KEY"] = self.minio_access_key
        if self.minio_secret_key:
            os.environ["MINIO_SECRET_KEY"] = self.minio_secret_key
        if self.minio_secure:
            os.environ["MINIO_SECURE"] = str(self.minio_secure)
        if self.minio_region:
            os.environ["MINIO_REGION"] = self.minio_region
        if self.minio_signature_ver:
            os.environ["MINIO_SIGNATURE_VER"] = self.minio_signature_ver


    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):  continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, update_fs: bool = True, **config):
        self.update_config(**config)
        self.set_env()

        # Reset the accessor to use the new settings
        from fileio.lib.posix.filesys import FileSysManager
        FileSysManager.get_accessor('minio', _reset=True)
    
    def build_s3fs_config(self) -> Dict[str, Any]:
        """
        Builds the s3fs config dict
        """
        config = {
            "client_kwargs": {
                "endpoint_url": self.minio_endpoint,
                "region_name": self.minio_region,
            },
            "config_kwargs": {
                "signature_version": self.minio_signature_ver,
            }
        }
        if self.minio_access_key:
            config["key"] = self.minio_access_key
        if self.minio_secret_key:
            config["secret"] = self.minio_secret_key
        if self.minio_access_token:
            config["token"] = self.minio_access_token
        if self.minio_config:
            config["config_kwargs"].update(self.minio_config)
        return config

class S3CompatSettings(BaseSettings):
    s3_compat_endpoint: Optional[str] = None
    s3_compat_access_key: Optional[str] = None
    s3_compat_secret_key: Optional[str] = None
    s3_compat_access_token: Optional[str] = None
    s3_compat_secure: Optional[bool] = True
    s3_compat_region: Optional[str] = None
    s3_compat_config: Optional[Union[str, Dict[str, Any]]] = None
    s3_compat_signature_ver: Optional[str] = 's3v4'

    class Config:
        env_prefix: str = ""

    @validator("s3_compat_config")
    def validate_s3_compat_config(cls, v) -> Dict:
        if v is None: return {}
        return json.loads(v) if isinstance(v, str) else v
    
    def set_env(self):
        if self.s3_compat_endpoint:
            os.environ["S3_COMPAT_ENDPOINT"] = self.s3_compat_endpoint
        if self.s3_compat_access_key:
            os.environ["S3_COMPAT_ACCESS_KEY"] = self.s3_compat_access_key
        if self.s3_compat_secret_key:
            os.environ["S3_COMPAT_SECRET_KEY"] = self.s3_compat_secret_key
        if self.s3_compat_access_token:
            os.environ["S3_COMPAT_ACCESS_TOKEN"] = self.s3_compat_access_token
        if self.s3_compat_secure:
            os.environ["S3_COMPAT_SECURE"] = str(self.s3_compat_secure)
        if self.s3_compat_region:
            os.environ["S3_COMPAT_REGION"] = self.s3_compat_region

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, **config):
        self.update_config(**config)
        self.set_env()
    

    def build_s3fs_config(self) -> Dict[str, Any]:
        """
        Builds the s3fs config dict
        """
        config = {
            "client_kwargs": {
                "endpoint_url": self.s3_compat_endpoint,
                "region_name": self.s3_compat_region,
            },
            "config_kwargs": {
                "signature_version": self.s3_compat_signature_ver,
            }
        }
        if self.s3_compat_access_key:
            config["key"] = self.s3_compat_access_key
        if self.s3_compat_secret_key:
            config["secret"] = self.s3_compat_secret_key
        if self.s3_compat_access_token:
            config["token"] = self.s3_compat_access_token
        if self.s3_compat_config:
            config["config_kwargs"].update(self.s3_compat_config)
        return config


class AzureSettings(BaseSettings):
    azure_account_name: Optional[str] = None
    azure_account_key: Optional[str] = None
    azure_sas_token: Optional[str] = None
    azure_connection_string: Optional[str] = None
    azure_blocksize: Optional[int] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    azure_tenant_id: Optional[str] = None
    azure_version_aware: Optional[bool] = False

    azure_secure: Optional[bool] = True
    azure_config: Optional[Union[str, Dict[str, Any]]] = None
    azure_socket_timeout: Optional[int] = None

    class Config:
        env_prefix: str = ""

    @validator("azure_config")
    def validate_azure_config(cls, v) -> Dict:
        if v is None: return {}
        return json.loads(v) if isinstance(v, str) else v
    
    def set_env(self):
        if self.azure_account_name:
            os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = self.azure_account_name
        if self.azure_account_key:
            os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = self.azure_account_key
        if self.azure_sas_token:
            os.environ["AZURE_STORAGE_SAS_TOKEN"] = self.azure_sas_token
        if self.azure_connection_string:
            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = self.azure_connection_string
        if self.azure_client_id:
            os.environ["AZURE_STORAGE_CLIENT_ID"] = self.azure_client_id
        if self.azure_client_secret:
            os.environ["AZURE_STORAGE_CLIENT_SECRET"] = self.azure_client_secret
        if self.azure_tenant_id:
            os.environ["AZURE_STORAGE_TENANT_ID"] = self.azure_tenant_id
        

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, **config):
        self.update_config(**config)
        self.set_env()
    
    def build_azurefs_config(self) -> Dict[str, Any]:
        """
        Builds the azurefs config dict
        """
        config = {
            "account_name": self.azure_account_name,
            "account_key": self.azure_account_key,
            "sas_token": self.azure_sas_token,
            "secure": self.azure_secure,
            "connection_string": self.azure_connection_string,
            "blocksize": self.azure_blocksize,
            "client_id": self.azure_client_id,
            "client_secret": self.azure_client_secret,
            "tenant_id": self.azure_tenant_id,
            "version_aware": self.azure_version_aware,
            "socket_timeout": self.azure_socket_timeout,
        }
        config = {k: v for k, v in config.items() if v is not None}
        if self.azure_config:
            config.update(self.azure_config)
        return config

class GithubSettings(BaseSettings):
    github_org: Optional[str] = None
    github_repo: Optional[str] = None
    github_user: Optional[str] = None
    github_token: Optional[str] = None
    github_sha: Optional[str] = None

    class Config:
        env_prefix: str = ""
        case_sensitive = False

    def set_env(self):
        if self.github_org:
            os.environ["GITHUB_ORG"] = self.github_org
        if self.github_repo:
            os.environ["GITHUB_REPO"] = self.github_repo
        if self.github_user:
            os.environ["GITHUB_USER"] = self.github_user
        if self.github_token:
            os.environ["GITHUB_TOKEN"] = self.github_token
        if self.github_sha:
            os.environ["GITHUB_SHA"] = self.github_sha

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, **config):
        self.update_config(**config)
        self.set_env()

    def build_githubfs_config(self) -> Dict[str, Any]:
        """
        Builds the githubfs config dict
        """
        config = {
            'org': self.github_org,
            'repo': self.github_repo,
            'user': self.github_user,
            'token': self.github_token,
            'sha': self.github_sha,
        }
        return {k: v for k, v in config.items() if v is not None}

class HuggingfaceSettings(BaseSettings):

    hf_token: Optional[str] = None
    huggingface_token: Optional[str] = None
    hugging_face_hub_token: Optional[str] = None

    hf_org: Optional[str] = None
    hf_user: Optional[str] = None
    hf_repo: Optional[str] = None

    huggingface_org: Optional[str] = None
    huggingface_user: Optional[str] = None
    huggingface_repo: Optional[str] = None

    hf_repo_id: Optional[str] = None
    huggingface_repo_id: Optional[str] = None

    hf_repo_type: Optional[str] = None
    huggingface_repo_type: Optional[str] = None

    class Config:
        env_prefix: str = ""
        case_sensitive = False

    @lazyproperty
    def token(self) -> str:
        return self.hf_token or self.huggingface_token or self.hugging_face_hub_token
    
    @lazyproperty
    def org(self) -> str:
        return self.hf_org or self.huggingface_org
    
    @lazyproperty
    def user(self) -> str:
        return self.hf_user or self.huggingface_user
    
    @lazyproperty
    def repo(self) -> str:
        return self.hf_repo or self.huggingface_repo
    
    @lazyproperty
    def repo_id(self) -> str:
        return (
            self.hf_repo_id
            or self.huggingface_repo_id
            or f"{self.org or self.user}/{self.repo}"
        )

    @lazyproperty
    def repo_type(self) -> str:
        return self.hf_repo_type or self.huggingface_repo_type
    
    def set_env(self):
        if self.token:
            os.environ["HUGGING_FACE_HUB_TOKEN"] = self.token
        if self.repo_id:
            os.environ["HF_REPO_ID"] = self.repo_id

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)


class CloudflareR2Settings(BaseSettings):
    r2_account_id: Optional[str] = None
    r2_access_key_id: Optional[str] = None
    r2_secret_access_key: Optional[str] = None
    r2_access_token: Optional[str] = None
    r2_region: Optional[str] = None

    r2_endpoint: Optional[str] = None
    r2_config: Optional[Union[str, Dict[str, Any]]] = None

    class Config:
        env_prefix: str = ""

    @root_validator(pre=True)
    def build_valid_values(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the values and build the valid values dict"""
        if values.get("r2_config") is not None:
            values["r2_config"] = json.loads(values["r2_config"]) if isinstance(values["r2_config"], str) else values["r2_config"]
        if values.get("r2_endpoint") is not None:
            values["r2_endpoint"] = values["r2_endpoint"].rstrip("/")
            if not values["r2_endpoint"].startswith("http"):
                values["r2_endpoint"] =  "https://" + values["r2_endpoint"]
        elif values.get("r2_account_id"):
            values["r2_endpoint"] = f"https://{values['r2_account_id']}.r2.cloudflarestorage.com"
        return values


    def set_env(self):
        if self.r2_endpoint:
            os.environ["R2_ENDPOINT"] = self.r2_endpoint
        if self.r2_account_id:
            os.environ["R2_ACCOUNT_ID"] = self.r2_account_id
        if self.r2_access_key_id:
            os.environ["R2_ACCESS_KEY_ID"] = self.r2_access_key_id
        if self.r2_secret_access_key:
            os.environ["R2_SECRET_ACCESS_KEY"] = self.r2_secret_access_key
        if self.r2_access_token:
            os.environ["R2_ACCESS_TOKEN"] = str(self.r2_access_token)

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, **config):
        self.update_config(**config)
        self.set_env()
    

    def build_s3fs_config(self) -> Dict[str, Any]:
        """
        Builds the s3fs config dict
        """
        config = {
            "client_kwargs": {
                "endpoint_url": self.r2_endpoint,
                "region_name": self.r2_region,
            },
            "config_kwargs": {}
        }
        if self.r2_access_key_id:
            config["key"] = self.r2_access_key_id
        if self.r2_secret_access_key:
            config["secret"] = self.r2_secret_access_key
        if self.r2_access_token:
            config["token"] = self.r2_access_token
        if self.r2_config:
            config["config_kwargs"].update(self.r2_config)
        return config
    

class WasabiS3Settings(BaseSettings):

    wasabi_access_key_id: Optional[str] = None
    wasabi_secret_access_key: Optional[str] = None
    wasabi_access_token: Optional[str] = None

    wasabi_region: Optional[str] = 'us-east-1'

    wasabi_endpoint: Optional[str] = None
    wasabi_config: Optional[Union[str, Dict[str, Any]]] = None

    class Config:
        env_prefix: str = ""

    @root_validator(pre=True)
    def build_valid_values(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the values and build the valid values dict"""
        if values.get("wasabi_config") is not None:
            values["wasabi_config"] = json.loads(values["wasabi_config"]) if isinstance(values["wasabi_config"], str) else values["wasabi_config"]
        if values.get("wasabi_endpoint") is not None:
            values["wasabi_endpoint"] = values["wasabi_endpoint"].rstrip("/")
            if not values["wasabi_endpoint"].startswith("http"):
                values["wasabi_endpoint"] =  "https://" + values["wasabi_endpoint"]
        else:
            values["wasabi_endpoint"] = f"https://s3.{values['wasabi_region']}.wasabisys.com" if values.get("wasabi_region", "") != "us-east-1" else \
                "https://s3.wasabisys.com"
        return values


    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            else:
                setattr(self, k, v)

    def update_auth(self, **config):
        self.update_config(**config)
        self.set_env()
    

    def set_env(self):
        if self.wasabi_endpoint:
            os.environ["WASABI_ENDPOINT"] = self.wasabi_endpoint
        if self.wasabi_access_key_id:
            os.environ["WASABI_ACCESS_KEY_ID"] = self.wasabi_access_key_id
        if self.wasabi_secret_access_key:
            os.environ["WASABI_SECRET_ACCESS_KEY"] = self.wasabi_secret_access_key
        if self.wasabi_access_token:
            os.environ["WASABI_ACCESS_TOKEN"] = self.wasabi_access_token
        if self.wasabi_region:
            os.environ["WASABI_REGION"] = self.wasabi_region

    def build_s3fs_config(self) -> Dict[str, Any]:
        """
        Builds the s3fs config dict
        """
        config = {
            "client_kwargs": {
                "endpoint_url": self.wasabi_endpoint,
                "region_name": self.wasabi_region,
            },
            "config_kwargs": {}
        }
        if self.wasabi_access_key_id:
            config["key"] = self.wasabi_access_key_id
        if self.wasabi_secret_access_key:
            config["secret"] = self.wasabi_secret_access_key
        if self.wasabi_access_token:
            config["token"] = self.wasabi_access_token
        if self.wasabi_config:
            config["config_kwargs"].update(self.wasabi_config)
        return config


class FileIOSettings(BaseSettings):

    read_chunk_size: Optional[int] = 1024 * 64 # 64KB
    url_chunk_size: Optional[int] = 1024 * 128 # 128KB
    
    num_workers: Optional[int] = 12
    checksum_cache_ttl: Optional[int] = 60 * 60 * 24 * 1 # 1 days
    enable_progress_bar: Optional[bool] = False
    tfio_enabled: Optional[bool] = False

    @lazyproperty
    def core(self) -> CoreSettings:
        """
        Returns the core settings
        """
        return CoreSettings()
    
    @lazyproperty
    def aws(self) -> AwsSettings:
        """
        Returns the aws settings
        """
        return AwsSettings()

    @lazyproperty
    def gcp(self) -> GcpSettings:
        """
        Returns the gcp settings
        """
        return GcpSettings()

    @lazyproperty
    def azure(self) -> AzureSettings:
        """
        Returns the azure settings
        """
        return AzureSettings()

    @lazyproperty
    def minio(self) -> MinioSettings:
        """
        Returns the minio settings
        """
        return MinioSettings()
    
    @lazyproperty
    def s3_compat(self) -> S3CompatSettings:
        """
        Returns the s3_compat settings
        """
        return S3CompatSettings()
    
    @lazyproperty
    def r2(self) -> CloudflareR2Settings:
        """
        Returns the r2 settings
        """
        return CloudflareR2Settings()
    
    @lazyproperty
    def wasabi(self) -> WasabiS3Settings:
        """
        Returns the wasabi settings
        """
        return WasabiS3Settings()
    
    @lazyproperty
    def github(self) -> GithubSettings:
        """
        Returns the github settings
        """
        return GithubSettings()
    
    @lazyproperty
    def huggingface(self) -> HuggingfaceSettings:
        """
        Returns the huggingface settings
        """
        return HuggingfaceSettings()

    def create_adc(
        self, 
        data: Union[str, Dict[str, Any]], 
        path: str = None
    ):
        """
        Create a new ADC based on the passed data and writes it to 
        path or GOOGLE_APPLICATION_CREDENTIALS
        """
        if isinstance(data, str): data = json.loads(data)
        path: pathlib.Path = pathlib.Path(path) if path else self.gcp.google_application_credentials
        path.write_text(json.dumps(data, indent = 2, ensure_ascii=False))
        self.gcp.google_application_credentials = path

    def get_boto_values(self):
        """
        Returns the boto config values
        """
        t = "[Credentials]\n"
        if self.aws.aws_access_key_id:
            t += f"aws_access_key_id = {self.aws.aws_access_key_id}\n"
        if self.aws.aws_secret_access_key:
            t += f"aws_secret_access_key = {self.aws.aws_secret_access_key}\n"
        if self.gcp.google_application_credentials.exists():
            t += f"gs_service_key_file = {self.gcp.google_application_credentials.as_posix()}\n"
        t += "\n[Boto]\n"
        t += "https_validate_certificates = True\n"
        t += "\n[GSUtil]\n"
        t += "content_language = en\n"
        t += "default_api_version = 2\n"
        if self.gcp.project: t += f"default_project_id = {self.gcp.project}\n"
        return t

    def write_botofile(
        self, 
        overwrite: bool = False, 
        **kwargs
    ):
        """
        Writes the boto config file to the path specified in the boto_config_path
        """
        if not self.core.boto_config_exists or overwrite:
            logger.info(f"Writing boto config to {self.core.boto_config_path.as_posix()}")
            self.core.boto_config_path.write_text(self.get_boto_values())

    def update_config(self, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            if isinstance(getattr(self, k), pathlib.Path):
                setattr(self, k, pathlib.Path(v))
            elif isinstance(getattr(self, k), BaseSettings):
                getattr(self, k).update_config(**v)
            else: setattr(self, k, v)

    def set_env(self):
        """
        Calls the set_env method on all the settings
        """
        self.aws.set_env()
        self.gcp.set_env()
        self.minio.set_env()
        self.s3_compat.set_env()
        # self.github.set_env()
        # self.huggingface.set_env()
        self.r2.set_env()
        self.wasabi.set_env()

    def update_auth(self, update_fs: bool = True, **config):
        """
        Updates the auth settings and optionally updates the fs
        """
        self.update_config(**config)
        self.set_env()

        if update_fs:
            # Reset the accessor to use the new settings
            from fileio.lib.posix.filesys import FileSysManager
            # from fileio.providers.filesys import get_accessor
            if config.get('aws'):
                FileSysManager.get_accessor('s3', _reset = True)
                # get_accessor('s3', _reset = True)
            if config.get('gcp'):
                FileSysManager.get_accessor('gs', _reset = True)
                # get_accessor('gs', _reset = True)
            if config.get('minio'):
                FileSysManager.get_accessor('minio', _reset = True)
                # get_accessor('minio', _reset = True)
            
            if config.get('s3_compat') or config.get('s3c'):
                FileSysManager.get_accessor('s3c', _reset = True)
                # get_accessor('s3_compat', _reset = True)
            
            if config.get('r2'):
                FileSysManager.get_accessor('r2', _reset = True)
                # get_accessor('r2', _reset = True)

            if config.get('wasabi'):
                FileSysManager.get_accessor('wasabi', _reset = True)
    

    class Config:
        env_prefix = "FILEIO_"
        case_sensitive = False


_fileio_settings: Optional[FileIOSettings] = None

def get_fileio_settings(**kwargs) -> FileIOSettings:
    """
    Returns the fileio settings
    """
    global _fileio_settings
    if _fileio_settings is None:
        _fileio_settings = FileIOSettings()
    if kwargs:
        _fileio_settings.update_config(**kwargs)
    return _fileio_settings

class ProxySettings:
    def __init__(self):
        self._settings = None

    def __getattr__(self, name):
        if self._settings is None:
            self._settings = get_fileio_settings()
        return getattr(self._settings, name)

settings: FileIOSettings = ProxySettings()


