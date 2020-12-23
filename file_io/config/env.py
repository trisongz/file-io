import os
from shutil import copyfile
import json
from subprocess import check_output
import threading
import dill as pickle
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from file_io.utils.logger import get_logger

_env_lock = threading.Lock()
_env_handler = None
_cloud_clients = None
_env_path = os.path.abspath(os.path.dirname(__file__))
_env_file = os.path.join(_env_path, 'cache/env.pkl')
_env_saved = os.path.exists(_env_file)
_env_loaded = False
logger = get_logger('FileIO')

def create_cloud_clients():
    import boto3
    from google.cloud import storage
    from google.oauth2 import service_account
    from azure.storage.blob import BlobServiceClient
    cloud = {}
    adc_key = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', None)
    if adc_key and os.path.exists(adc_key):
        cloud['gcp_client'] = service_account.Credentials.from_service_account_file(adc_key)
        cloud['gcs_client'] = storage.Client.from_service_account_json(adc_key)
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", None)
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
    if aws_key and aws_secret:
        cloud['s3_client'] = boto3.Session(
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret
            )
    else:
        cloud['s3_client'] = boto3.Session()
    azure_key = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", None)
    if azure_key:
        cloud['azure_client'] = BlobServiceClient.from_connection_string(azure_key)
    else:
        cloud['azure_client'] = None
    ssh_uname = os.environ.get("FILEIO_SSH_USER", None)
    ssh_pword = os.environ.get("FILEIO_SSH_PASS", None)
    if ssh_uname:
        cloud['ssh'] = ssh_uname
        if ssh_pword:
            cloud['ssh'] = cloud['ssh'] + ':' + ssh_pword
    else:
        cloud['ssh'] = None
    return cloud

def configure_cloud_clients():
    global _cloud_clients
    if _cloud_clients:
        return
    _cloud_clients = create_cloud_clients()

def get_cloud_clients():
    configure_cloud_clients()
    return _cloud_clients

class Env:
    def __init__(self):
        self.init_env()
    
    def init_env(self):
        self.path = _env_path
        self.filename = _env_file
        self.home = os.environ.get("HOME", "/root")
        self.files = {
            'adc': os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", None),
            'boto': os.environ.get("BOTO_PATH", None),
        }
        self.vars = {
            'GOOGLE_APPLICATION_CREDENTIALS': self.files['adc'],
            'BOTO_PATH': self.files['boto'],
            'TF_CPP_MIN_LOG_LEVEL': '3',
            'AWS_ACCESS_KEY_ID': os.environ.get("AWS_ACCESS_KEY_ID", None),
            'AWS_SECRET_ACCESS_KEY': os.environ.get("AWS_SECRET_ACCESS_KEY", None)
        }
        self.vals = {}
    
    def configure_env(self):
        self.configure_imports()
        self.configure_auths()
        self.configure_cloud()
        self.set_env()

    def configure_cloud(self):
        get_cloud_clients()

    def configure_imports(self, silent=True):
        from file_io.config.imports import check_imports
        self.vals['avail'] =  check_imports()
        if not silent:
            for lib, avail in self.vals['avail']['libs'].items():
                logger.info(f"{lib}: {avail}")
        if self.vals['avail']['colab']:
            self.vals['colab'] = True

    def configure_auths(self):
        default_boto_path = os.path.join(self.home, '.boto')
        if self.files['boto'] and os.path.exists(self.files['boto']) and not os.path.exists(default_boto_path):
            copyfile(self.files['boto'], default_boto_path)
        self.set_env()

    def set_env(self, silent=True):
        for var in self.vars:
            if self.vars[var]:
                if not silent:
                    logger.info(f"{var} = {self.vars[var]}")
                os.environ[var] = str(self.vars[var])
    
    def set_adc(self, adc=None):
        if adc and os.path.exists(adc):
            self.files['adc'] = adc
            self.vars['GOOGLE_APPLICATION_CREDENTIALS'] = adc
        if self.files['adc'] and os.path.exists(self.files['adc']):
            logger.info(f"Setting ADC to Default: {self.files['adc']}")
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.files['adc']

    def save(self):
        #logger.info(f"Saving Trainer Environment to {self.filename}")
        with open(self.filename, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load(cls, filename):
        global _env_loaded
        if not _env_loaded:
            #logger.info(f"Loading Cached Environment from {filename}")
            _env_loaded = True
        with open(filename, 'rb') as f:
            return pickle.load(f)

    def __call__(self, val):
        return self.vals.get(val, None)

def _setup_env(reset):
    if _env_saved:
        _env = Env.load(_env_file)
        if reset:
            _env.configure_env()
            _env.save()
        _env.set_env()
    else:
        _env = Env()
        _env.configure_env()
        _env.save()
    return _env

def _configure_env(reset):
    global _env_handler
    with _env_lock:
        if _env_handler and not reset:
            return
        _env_handler = _setup_env(reset)

def get_env(reset=False):
    reset = bool(os.environ.get('RESET_FILEIO_ENV', reset))
    _configure_env(reset)
    return _env_handler