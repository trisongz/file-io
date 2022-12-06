import os
import json
import pathlib
import multiprocessing as mp
from pydantic import BaseSettings, validator

from typing import Optional, Dict, Any, Union

from fileio.utils.logs import default_logger as logger
from fileio.types.classprops import lazyproperty

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
        try:
            from google.colab import drive
            return True
        except ImportError:
            return False
    
    def set_env(self):
        if self.boto_config_exists:
            os.environ["BOTO_CONFIG"] = self.boto_config_path.as_posix()
            os.environ["BOTO_PATH"] = self.boto_config_path.as_posix()
    
    def update_config(self, **kwargs):
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
    set_s3_endpoint: Optional[bool] = True
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
            # Reset the accessor to use the new settings
            from fileio.providers.filesys import get_accessor
            get_accessor('s3', _reset=True)

    def build_s3fs_config(self) -> Dict[str, Any]:
        """
        Builds the s3fs config dict
        """
        config = {}
        if self.aws_access_key_id:
            config["key"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            config["secret"] = self.aws_secret_access_key
        if self.aws_access_token:
            config["token"] = self.aws_access_token
        if not core_settings.boto_config_exists:
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
            from fileio.providers.filesys import get_accessor
            get_accessor('gs', _reset=True)

    

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
        from fileio.providers.filesys import get_accessor
        get_accessor('minio', _reset=True)
    
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
    s3_compat_secure: Optional[bool] = True
    s3_compat_region: Optional[str] = None
    s3_compat_config: Optional[Union[str, Dict[str, Any]]] = None

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



class Settings(BaseSettings):

    @lazyproperty
    def core(self) -> CoreSettings:
        return CoreSettings()
    
    @lazyproperty
    def aws(self) -> AwsSettings:
        return AwsSettings()

    @lazyproperty
    def gcp(self) -> GcpSettings:
        return GcpSettings()
    
    @lazyproperty
    def minio(self) -> MinioSettings:
        return MinioSettings()
    
    @lazyproperty
    def s3_compat(self) -> S3CompatSettings:
        return S3CompatSettings()

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
        self.aws.set_env()
        self.gcp.set_env()
        self.minio.set_env()
        self.s3_compat.set_env()

    def update_auth(self, update_fs: bool = True, **config):
        self.update_config(**config)
        self.set_env()

        if update_fs:
            # Reset the accessor to use the new settings
            from fileio.providers.filesys import get_accessor
            if config.get('aws'):
                get_accessor('s3', _reset = True)
            if config.get('gcp'):
                get_accessor('gs', _reset = True)
            if config.get('minio'):
                get_accessor('minio', _reset = True)
    

settings = Settings()