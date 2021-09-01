



JSON_FUNC = None
JSON_PARSER = None
PICKLE_FUNC = None
PT_FUNC = None
TF_FUNC = None
TF_COMPAT_v1 = None
TF_COMPAT_v2 = None
GCS_FUNC = None
SOPEN_FUNC = None

GCS_METHOD = None
GCS_UNAVAILABLE_EXCEPTIONS = (Exception)

SUPABASE_FUNC = None

PT_DEVICE = None

try:
    import tensorflow as tf
    TF_COMPAT_v1 = tf.compat.v1
    TF_COMPAT_v2 = tf.compat.v2
    TF_FUNC = tf
    GCS_FUNC = tf.io.gfile
    GCS_METHOD = 'tf'
except ImportError:
    from smart_open.gcs import open as gcs_open
    GCS_FUNC = gcs_open
    GCS_METHOD = 'sm'
try:
    import torch
    PT_FUNC = torch
    PT_DEVICE = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
except ImportError:
    pass

try:
    import dill as _pickler
    PICKLE_FUNC = _pickler
except ImportError:
    import pickle as pkl
    PICKLE_FUNC = pkl
try:
    from supabase_py import create_client as _create_client
    SUPABASE_FUNC = _create_client
except ImportError:
    pass


if TF_FUNC:
    GCS_UNAVAILABLE_EXCEPTIONS = (
        tf.errors.UnimplementedError,
        tf.errors.FailedPreconditionError,
        tf.errors.PermissionDeniedError,
        tf.errors.AbortedError,
    )

import simdjson as _json
import smart_open as _smart_open

JSON_FUNC = _json
JSON_PARSER = _json.Parser()
SOPEN_FUNC = _smart_open



