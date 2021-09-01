


import os
from fileio.cloud import auth
from typing import Union, Dict

from google.cloud import storage
from fileio.cloud.auth import TransportParams
from fileio.configs import CloudConfig
from fileio.static.classes import GCSConfig
from fileio.core.py_utils import safe_urlsplit, check_kwargs
from google.cloud.storage import Blob as GCSBlob
from google.cloud.exceptions import NotFound as GCSNotFound

def _auth_gcp_gcs(
    gcp_adc: Union[str, Dict[str, str]] = CloudConfig.GoogleAuth,
    gcp_project: str = CloudConfig.GoogleProject,
    gcp_zone: str = CloudConfig.GoogleZone
    ):
    auth.GOOGLE_GCS_AUTH = GCSConfig(gcp_adc, gcp_project, gcp_zone)
    if gcp_adc:
        if os.path.exists(gcp_adc):
            auth.GOOGLE_GCS_CLIENT = storage.Client.from_service_account_json(gcp_adc)
        elif isinstance(gcp_adc, dict):
            auth.GOOGLE_GCS_CLIENT = storage.Client.from_service_account_info(**gcp_adc)
        else:
            auth.GOOGLE_GCS_CLIENT = storage.Client.from_service_account_info(info=gcp_adc)
    if not auth.GOOGLE_GCS_CLIENT: storage.Client()

SCHEME = "gs"

def parse_uri(uri_as_string):
    sr = safe_urlsplit(uri_as_string)
    assert sr.scheme == SCHEME
    bucket_id = sr.netloc
    blob_id = sr.path.lstrip('/')
    return dict(scheme=SCHEME, bucket_id=bucket_id, blob_id=blob_id)


def open_gcs(uri, mode, transport_params = None):
    parsed_uri = parse_uri(uri)
    if not auth.GOOGLE_GCS_AUTH: _auth_gcp_gcs()
    transport_params = transport_params or TransportParams.GCP
    kwargs = check_kwargs(gcs_open, transport_params)
    return gcs_open(parsed_uri['bucket_id'], parsed_uri['blob_id'], mode, **kwargs)

def gcs_open(bucket_id, blob_id, mode, client, chunk_size=None, encoding=None, errors=None, newline=None):
    bucket = client.bucket(bucket_id)
    blob = bucket.get_blob(blob_id)
    return blob.open(mode, chunk_size, encoding, errors, newline)


