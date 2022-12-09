# file-io

[![PyPI version](https://badge.fury.io/py/file-io.svg)](https://badge.fury.io/py/file-io)

 Drop in replacement for `pathlib.Path` with support for Cloud Object Storage with async compatability. 

 Supports: 
 - `gs://` - Google Cloud Storage
 - `s3://` - Amazon S3
 - `file://` - Local Filesystem
 - `minio://` - Minio Object Storage

## Quickstart

### Installation

```bash

# From Source
pip install --upgrade git+https://github.com/trisongz/file-io.git
# Stable
pip install --upgrade file-io

```

### Quick Usage

```python

from fileio import File

s3_bucket = 's3://my-bucket
gcs_bucket = 'gs://my-bucket'

# The File class automatically routes the path to the correct filesystem
s3_path = File(s3_bucket)
gcs_path = File(gcs_bucket)

file_name = 'test.txt'
file_data = 'hello world'

# Use joinpath to create a new file path just like pathlib.Path
# It will also ensure the path is valid for the given filesystem

s3_file_path = s3_path.joinpath(file_name)
gcs_file_path = gcs_path.joinpath(file_name)

# Show posix path
print('S3 File: ', s3_file_path.as_posix())
print('GCS File: ', gcs_file_path.as_posix())

# Write to the file
s3_file_path.write_text(file_data)
gcs_file_path.write_text(file_data)

# Write Bytes/Binary
# s3_file_path.write_bytes(file_data)
# gcs_file_path.write_bytes(file_data)

# Get File Info
print(s3_file_path.info())
print(gcs_file_path.info())

# Read from the file
print(s3_file_path.read_text())
print(gcs_file_path.read_text())

# Read Bytes/Binary
# print(s3_file_path.read_bytes())
# print(gcs_file_path.read_bytes())

# Validate the file exists
print(s3_file_path.exists())
print(gcs_file_path.exists())

# Delete the file
s3_file_path.unlink()
# s3_file_path.rm_file()
# s3_file_path.rm()

gcs_file_path.unlink()
# gcs_file_path.rm_file()
# gcs_file_path.rm()

# Use as standard open method
with s3_file_path.open('w') as f:
    f.write(file_data)

with gcs_file_path.open('w') as f:
    f.write(file_data)

# Search with glob
print(s3_path.glob('*.txt'))
print(gcs_path.glob('*.txt'))

```


### Async Usage

Additional Async capaibilities are available with most methods using `async_` prefix. This will allow you to use the async context manager and async file methods.

```python

import asyncio
from fileio import File


s3_bucket = 's3://my-bucket
gcs_bucket = 'gs://my-bucket'

s3_path = File(s3_bucket)
gcs_path = File(gcs_bucket)

file_name = 'test.txt'
file_data = 'hello world'

# Use joinpath to create a new file path just like pathlib.Path
# It will also ensure the path is valid for the given filesystem

s3_file_path = s3_path.joinpath(file_name)
gcs_file_path = gcs_path.joinpath(file_name)

print('S3 File: ', s3_file_path.as_posix())
print('GCS File: ', gcs_file_path.as_posix())

async def run_tests():
    # All methods shown above are also available as async methods

    # Write to the file
    await s3_file_path.async_write_text(file_data)
    await gcs_file_path.async_write_text(file_data)

    # Write Bytes/Binary
    # await s3_file_path.async_write_bytes(file_data)
    # await gcs_file_path.async_write_bytes(file_data)

    # Read from the file
    print(await s3_file_path.async_read_text())
    print(await gcs_file_path.async_read_text())

    # Read Bytes/Binary
    # print(await s3_file_path.async_read_bytes())
    # print(await gcs_file_path.async_read_bytes())

    # Validate the file exists
    print(await s3_file_path.async_exists())
    print(await gcs_file_path.async_exists())

    # Delete the file
    await s3_file_path.async_unlink()
    # await s3_file_path.async_rm_file()
    # await s3_file_path.async_rm()

    await gcs_file_path.async_unlink()
    # await gcs_file_path.async_rm_file()
    # await gcs_file_path.async_rm()

    # With async, you need to use the async context manager
    async with s3_file_path.async_open('w') as f:
        # note that the `write` method requires `await`
        await f.write(file_data)

    async with gcs_file_path.async_open('w') as f:
        await f.write(file_data)
    
    # Search with glob
    print(await s3_path.async_glob('*.txt'))
    print(await gcs_path.async_glob('*.txt'))
    


asyncio.run(run_tests())

```

### Configuration

The configuration for cloud providers are picked up automatically, however if you need to configure them, you can do so using the `settings` and set them explicitly.

```python

from fileio import File, settings

# Configure S3
settings.aws.update_auth(
    aws_access_token = 'my-access-token',
    aws_access_key_id = 'my-access-key-id',
    aws_secret_access_key = 'my-secret-access-key',
    aws_region = "us-east-1"
    set_s3_endpoint = True
)

# Configure GCS
settings.gcp.update_auth(
    gcp_project = 'my-project',
    google_application_credentials = 'my-credentials.json'
)

# Configure Multiple
# Auths and reset the underlying filesystems
# to use the new auths.

settings.update_auth(
    gcp = {
        'gcp_project': 'my-project',
        'google_application_credentials': 'my-credentials.json'
    },
    aws = {
        'aws_access_key_id': 'my-access'
    },
    minio = {
        'minio_endpoint': 'https://my-endpoint',
    }
)


```

### Useful Tips and Tricks

Below are a few snippets of code that may be useful for common tasks.

```python
from fileio import File

## Easily copy async files
async def clone_file(src, dst):
    # Ensure they are both `FileType` objects
    # This will read the file from the src and write it to the dst
    # in binary mode, so it can clone `s3` <> `gcs` files
    src, dst = File(src), File(dst)
    await dst.async_write_bytes(await src.async_read_bytes())
    return dst

async def file_checksum(src):
    # Ensure it is a `FileType` object
    src = File(src)
    # Read the file in chunks and calculate the checksum
    # This is useful for large files
    # can use any standard hashlib method
    return await src.async_get_checksum(
        method = 'md5'
        chunk_size = 1024 * 4
    )

async def copy_uploaded_file(src: 'UploadFile'):
    # Useful for copying files uploaded via FastAPI / Starlette
    # since they are often `SpooledTemporaryFile` objects
    # which means for tasks such as checksuming before
    # doing any processing, you need to copy the file
    # otherwise it will be removed from memory. 

    # Remember to remove the file after it is done
    dst = File.get_tempfile(
        delete = False
    )
    # Copy the file
    await dst.async_write_bytes(await src.read())
    return dst 


```


### Dependencies

The aim of this library is to be as lightweight as possible. It is built on top of the following libraries, and leverages lazyloading of dependencies to avoid unnecessary imports:

- [fsspec](https://github.com/fsspec/fsspec) - For Filesystem Support

- [s3fs](https://github.com/fsspec/s3fs) - For S3 Support

- [gcsfs](https://github.com/fsspec/gcsfs) - For GCS Support

    - [tensorflow](https://github.com/tensorflow/tensorflow) - Additional GCS Support but only if already available. Leverages tf's better C++ bindings for GCS.

- [loguru](https://github.com/Delgan/loguru) - Logging

- [pydantic](https://github.com/pydantic/pydantic) - Type Support and Configuration

- [dill](https://github.com/uqfoundation/dill) - Serialization Support

- [aiofile](http://github.com/mosquito/aiofile) - Async File Support

- [anyio](https://github.com/agronholm/anyio) - Async Support


### [Changelogs](changelogs.txt)

**v0.4.1**

- Modified and validated `settings` to enable multiple auths and reset the underlying filesystems to use the new auths.

- Update readme with better examples and documentation.


