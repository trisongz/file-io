import os
import json
from fileio.static.helpers import classproperty


SRC_BASE_URL = 'https://github.com/trisongz/file-io/tree/master/'
DATA_DIR = os.environ.get('FILEIO_DATA_DIR', os.path.join('~', 'fileio'))
INCOMPLETE_SUFFIX = '.incomplete'

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ROOT_DIR, '.auth_configs.json')
USER_CONFIG_PATH = os.environ.get('FILEIO_CONFIG', '')

ENABLE_PROGRESS_BAR = False

GCS_ROOT_DIR = 'gs://tfds-data'
GCS_DATASET_INFO_DIR = 'dataset_info'
GCS_DATASETS_DIR = 'datasets'
GCS_DISABLED = False


class CloudConfig:
    GoogleAuth = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    GoogleProject = os.environ.get('GOOGLE_CLOUD_PROJECT')
    GoogleBucket = os.environ.get('GOOGLE_BUCKET')
    GoogleZone = os.environ.get('GOOGLE_ZONE')
    SupabaseURL = os.environ.get('SUPABASE_URL')
    SupabaseKey = os.environ.get('SUPABASE_KEY')
    AWSAccessID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWSAccessKey = os.environ.get('AWS_SECRET_ACCESS_KEY')
    AWSRegion = os.environ.get('AWS_DEFAULT_REGION')
    S3Bucket = os.environ.get('S3_BUCKET')
    has_loaded = False

    @classmethod
    def update(cls, **configs):
        if not configs: return
        for provider, var in configs.items():
            if getattr(CloudConfig, provider) and var:
                setattr(CloudConfig, provider, var)
        CloudConfig.save()
        CloudConfig.set_environ()
    
    @classmethod
    def save(cls):
        data = json.load(open(CONFIG_PATH, 'r')) if os.path.exists(CONFIG_PATH) else {}
        for key, val in CloudConfig.__dict__.items():
            if data.get(key) and not val:
                continue
            if not data.get(key) and val:
                data[key] = val
        json.dump(open(CONFIG_PATH, 'w'), indent=2, ensure_ascii=False)
    
    @classmethod
    def load(cls, load_path = None):
        load_path = load_path or CONFIG_PATH
        assert os.path.exists(load_path), 'load_path does not exist'
        configs = json.load(open(CONFIG_PATH, 'r'))
        for provider, var in configs.items():
            if getattr(CloudConfig, provider) and var:
                setattr(CloudConfig, provider, var)
        CloudConfig.set_environ()
        
    @classmethod
    def set_environ(cls):
        if CloudConfig.GoogleAuth: 
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CloudConfig.GoogleAuth
        if CloudConfig.GoogleProject:
            os.environ['GOOGLE_CLOUD_PROJECT'] = CloudConfig.GoogleProject
        if CloudConfig.GoogleBucket:
            os.environ['GOOGLE_BUCKET'] = CloudConfig.GoogleBucket
        if CloudConfig.GoogleZone:
            os.environ['GOOGLE_ZONE'] = CloudConfig.GoogleZone
        if CloudConfig.SupabaseURL:
            os.environ['SUPABASE_URL'] = CloudConfig.SupabaseURL
        if CloudConfig.SupabaseKey:
            os.environ['SUPABASE_KEY'] = CloudConfig.SupabaseKey
        if CloudConfig.AWSAccessID:
            os.environ['AWS_ACCESS_KEY_ID'] = CloudConfig.AWSAccessID
        if CloudConfig.AWSAccessKey:
            os.environ['AWS_SECRET_ACCESS_KEY'] = CloudConfig.AWSAccessKey
        if CloudConfig.AWSRegion:
            os.environ['AWS_DEFAULT_REGION'] = CloudConfig.AWSRegion
            # https://github.com/tensorflow/tensorflow/issues/38054
            os.environ['AWS_REGION'] = CloudConfig.AWSRegion
            os.environ['S3_ENDPOINT'] = f'https://s3-{CloudConfig.AWSRegion}.amazonaws.com'
            os.environ['S3_VERIFY_SSL'] = 0
        if CloudConfig.S3Bucket:
            os.environ['S3_BUCKET'] = CloudConfig.S3Bucket
        

    @classmethod
    def run_init(cls):
        if CloudConfig.has_loaded: return
        if os.path.exists(USER_CONFIG_PATH):
            CloudConfig.load(USER_CONFIG_PATH)
        elif os.path.exists(CONFIG_PATH):
            CloudConfig.load(CONFIG_PATH)
        else:
            CloudConfig.set_environ()
        CloudConfig.has_loaded = True
        
CloudConfig.run_init()

class CloudProviders(object):

    @classproperty
    def GoogleCloudStorage(cls):
        return bool(CloudConfig.GoogleAuth is not None)
    
    @classproperty
    def AWSS3Storage(cls):
        return bool(CloudConfig.AWSAccessID is not None and CloudConfig.AWSAccessKey is not None)
    
    @classproperty
    def SupabaseStorage(cls):
        return bool(CloudConfig.SupabaseURL is not None and CloudConfig.SupabaseKey is not None)

if __name__ == '__main__':
    print('GCS: ', CloudProviders.GoogleCloudStorage)
    print('AWS S3: ', CloudProviders.AWSS3Storage)
    print('Supabase: ', CloudProviders.SupabaseStorage)





