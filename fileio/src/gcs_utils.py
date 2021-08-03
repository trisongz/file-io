# Refactoring base Lib from TFDS

"""
adapted from https://github.com/tensorflow/datasets/blob/v4.4.0/tensorflow_datasets/core/utils/gcs_utils.py
"""


"""Utilities for accessing TFDS GCS buckets."""

import concurrent.futures
import os
import posixpath
from typing import List, Optional


from ..utils import lazy_import

tf = lazy_import('tensorflow.compat.v2')

from fileio.src import type_utils
from fileio.src import generic_path
from fileio.src import py_utils


GCS_ROOT_DIR = 'gs://tfds-data'

# for dataset_info/
GCS_DATASET_INFO_DIR = 'dataset_info'
GCS_DATASETS_DIR = 'datasets'

_is_gcs_disabled = False

# Exception raised when GCS isn't available
# * UnimplementedError: On windows, gs:// isn't supported on old TF versions.
#     https://github.com/tensorflow/tensorflow/issues/38477
# * FailedPreconditionError: (e.g. no internet)
# * PermissionDeniedError: Some environments block GCS access.
# * AbortedError: All 10 retry attempts failed.
GCS_UNAVAILABLE_EXCEPTIONS = (
    tf.errors.UnimplementedError,
    tf.errors.FailedPreconditionError,
    tf.errors.PermissionDeniedError,
    tf.errors.AbortedError,
)


def gcs_path(*relative_path: type_utils.PathLike, gcs_bucket=None, overwrite_global=False) -> type_utils.ReadWritePath:
    """Returns the GCS URI path.
    Args:
        *relative_path: Eventual relative path in the bucket.
    Returns:
        path: The GCS uri.
    """
    global GCS_ROOT_DIR
    bucket_path = GCS_ROOT_DIR or gcs_bucket
    if gcs_bucket and overwrite_global:
        GCS_ROOT_DIR = generic_path.as_path(gcs_bucket).parent
    return generic_path.as_path(bucket_path).joinpath(*relative_path)


# Community datasets index.
# This file contains the list of all community datasets with their associated
# location.
# Datasets there are downloaded and installed locally by the
# `PackageRegister` during `tfds.builder`
GCS_COMMUNITY_INDEX_PATH = gcs_path() / 'community-datasets-list.jsonl'


def exists(path: type_utils.ReadWritePath) -> bool:
    """Checks if path exists. Returns False if issues occur connecting to GCS."""
    try:
        return path.exists()
    except GCS_UNAVAILABLE_EXCEPTIONS:
        return False


@py_utils.memoize()
def gcs_listdir(dir_name: str) -> Optional[List[str]]:
    """List all files in the given GCS dir (`['dataset/1.0.0/file0', ...]`)."""
    root_dir = gcs_path(dir_name)
    if _is_gcs_disabled or not exists(root_dir):
        return None
    return [posixpath.join(dir_name, f.name) for f in root_dir.iterdir()]
