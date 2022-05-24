

"""
Cloud Provider Configs
"""

import os
import json
import pathlib
from typing import Optional, Union, Any, Dict, TYPE_CHECKING

from pydantic import Json
from loguru import logger

from ..types.config import ConfigModel


try:
    from google.colab import drive
    _is_colab = True
except ImportError: _is_colab = False

class CloudConfig(ConfigModel):
    config_dir: Optional[pathlib.Path] = pathlib.Path("~/.auth").resolve()
    boto_config: Optional[pathlib.Path] = pathlib.Path("~/.boto")
    
    """ 
    AWS Specific 
    """
    aws_access_token: Optional[str] = ""
    aws_access_key_id: Optional[str] = ""
    aws_secret_access_key: Optional[str] = ""
    aws_region: Optional[str] = "us-east-1"
    set_s3_endpoint: Optional[bool] = True
    s3_config: Optional[Json] = None

    """ 
    GCP Specific 
    """

    gcp_project: Optional[str] = ""
    gcloud_project: Optional[str] = ""
    google_cloud_project: Optional[str] = ""
    google_application_credentials: Optional[pathlib.Path] = pathlib.Path("~/adc.json").resolve()

    gcs_client_config: Optional[Json] = None
    gcs_config: Optional[Json] = None

    """
    Minio Specific
    """
    minio_endpoint: Optional[str] = ""
    minio_access_key: Optional[str] = ""
    minio_secret_key: Optional[str] = ""
    minio_access_token: Optional[str] = ""
    minio_config: Optional[Json] = None
    minio_signature_ver: str = 's3v4'

    """
    S3-Compatiable Generic
    """
    s3compat_endpoint: Optional[str] = ""
    s3compat_region: Optional[str] = ""
    s3compat_access_key: Optional[str] = ""
    s3compat_secret_key: Optional[str] = ""
    s3compat_access_token: Optional[str] = ""
    s3compat_config: Optional[Json] = None

    def create_adc(self, data: Union[str, Dict[str, Any]], path: str = None):
        """
        Create a new ADC based on the passed data and writes it to 
        path or GOOGLE_APPLICATION_CREDENTIALS
        """
        if isinstance(data, str): data = json.loads(data)
        path: pathlib.Path = pathlib.Path(path) if path else self.google_application_credentials
        path.write_text(json.dumps(data, indent = 2, ensure_ascii=False))
        self.google_application_credentials = path
        

    def get_gcp_project(self):
        for v in [self.gcp_project, self.gcloud_project, self.google_cloud_project]:
            if v: return v

    def get_boto_config_path(self):
        if not self.boto_config:
            self.boto_config = pathlib.Path('/root/.boto') if _is_colab else pathlib.Path('~/.boto').resolve(True)
        return self.boto_config

    def get_s3_endpoint(self):
        return f'https://s3.{self.aws_region}.amazonaws.com'

    def get_boto_path(self):
        boto_config = self.get_boto_config_path()
        if boto_config.exists(): return boto_config
        self.config_dir.mkdir(create_parents=True, exist_ok=True)
        return self.config_dir.joinpath('.boto')
    
    def should_write_boto(self):
        boto_config = self.get_boto_config_path()
        return not boto_config.exists()
 
    def get_boto_values(self):
        t = "[Credentials]\n"
        if self.aws_access_key_id:
            t += f"aws_access_key_id = {self.aws_access_key_id}\n"
            t += f"aws_secret_access_key = {self.aws_secret_access_key}\n"
        if self.google_application_credentials.exists():
            t += f"gs_service_key_file = {self.google_application_credentials.as_posix()}\n"
        t += "\n[Boto]\n"
        t += "https_validate_certificates = True\n"
        t += "\n[GSUtil]\n"
        t += "content_language = en\n"
        t += "default_api_version = 2\n"
        if gcp_project := self.get_gcp_project():
            t += f"default_project_id = {gcp_project}\n"
        return t


    def write_botofile(self, overwrite: bool = False, **kwargs):
        if self.should_write_boto:
            p = self.get_boto_path()
            if not p.exists() or overwrite:
                logger.info(f'Writing Botofile to {p.as_posix()}')
                p.write_text(self.get_boto_values())
            else: 
                logger.error(f'Botofile {p.as_posix()} exists and overwrite is False. Not overwriting')
            return p
        else: 
            logger.warning(f'Skipping writing Botofile as BotoConfig = {self.boto_config.as_posix()} exists')
            return self.boto_config


    def set_auth_env(self):
        if self.google_application_credentials.exists():
            os.system['GOOGLE_APPLICATION_CREDENTIALS'] = self.google_application_credentials.as_posix()
        gcp_project = self.get_gcp_project()
        if gcp_project: 
            os.system['GOOGLE_CLOUD_PROJECT'] = gcp_project

        try:
            botopath = self.get_boto_path()

            ## We know this is our custom botofile
            if botopath.exists() and self.should_write_boto():
                os.system['BOTO_PATH'] = botopath.as_posix()
                os.system['BOTO_CONFIG'] = botopath.as_posix()
        except: pass
        if self.aws_access_key_id:
            os.system['AWS_ACCESS_KEY_ID'] = self.aws_access_key_id
            os.system['AWS_SECRET_ACCESS_KEY'] = self.aws_secret_access_key
        
        if self.minio_endpoint:
            os.system['MINIO_ENDPOINT'] = self.minio_endpoint
        if self.set_s3_endpoint:
            os.system['S3_ENDPOINT'] = self.get_s3_endpoint()
        if self.minio_access_key:
            os.system['MINIO_ACCESS_KEY'] = self.minio_access_key
            os.system['MINIO_SECRET_KEY'] = self.minio_secret_key
        

    def update_auth(self, **config):
        self.update_config(**config)
        self.set_auth_env()
    
    class Config:
        env_prefix = ""
        


        