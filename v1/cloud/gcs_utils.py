import posixpath
from typing import List, Optional

from fileio.core import type_utils
from fileio.core import generic_path
from fileio.core import py_utils
from fileio.configs import GCS_ROOT_DIR, GCS_DATASET_INFO_DIR,  GCS_DATASETS_DIR, GCS_DISABLED

from fileio.core.libs import GCS_UNAVAILABLE_EXCEPTIONS

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
    if GCS_DISABLED or not exists(root_dir):
        return None
    return [posixpath.join(dir_name, f.name) for f in root_dir.iterdir()]
