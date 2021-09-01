
from fileio.static.helpers import classproperty

AWS_S3_AUTH = None
AWS_S3_SESSION = None
GOOGLE_GCS_AUTH = None
GOOGLE_GCS_CLIENT = None
SUPABASE_AUTH = None
SUPABASE_CLIENT = None

class TransportParams(object):

    @classproperty
    def AWS(cls):
        if AWS_S3_SESSION: return {'client': AWS_S3_SESSION.client('s3')}
        return {}

    @classproperty
    def GCP(cls):
        if GOOGLE_GCS_CLIENT: return {'client': GOOGLE_GCS_CLIENT}
        return {}
