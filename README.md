# file_io
 Deterministic File Lib to make working with Files across Object Storage easier


## Quickstart

```python3
!pip install --upgrade git+https://github.com/trisongz/file_io.git
!pip install --upgrade file-io


from file_io import File
import json

input_file = 'yourfile.json' # alternatively ['file1.json', 'file2.json']
output_file = 'outfile.json'

def loader(line):
    return json.loads(line)

def writer(line):
    return json.dumps(line, ensure_ascii=False)

f = File(input_file, binary=False)

f.set_reader(loader)
f.set_writer(writer)

f.open_writer(output_file, binary=False, overwrite=False)

for line in f:
    f.write(line, newline='\n')

f.close()
print(f.stats())

# {
#    'yourfile.json': {'read': 159799}, 
#    'outfile.json': {'write': 159799}
# }

```
## Supported File Storages

- Google Cloud Storage (gs://)
- S3 Object Storage (s3://)
- HTTP/HTTPS
- HDFS/WEBHDFS
- local filesystem

## Deterministic Elements

- append vs write. For any local and GCS objects, if overwrite is not called explicitly and the file exists, write mode is set to append
- binary mode. Binary is not enabled by default. Passing binary=True will enable binary mode.
- f.write writes both the passed text as well as writing a newline.
- newline for f.write is defaulted to '\n'. If newline=None, each write will be continuous.
- The functions passed to f.set_reader and f.set_writer will be invoked during each pass of a new item. If none are set, the item will be returned/written as is.
- Input files invoked during first call of File can be either a list or string. When the iterator is called, it will iterate through all the provided files.
- flushing occurs every 500 writes. This is to ensure writing to object storage file and not losing any that remains in buffer.
- f.close() needs to be explicitly called to close the write file if not using a with call. This calls fn.flush() and fn.close()
- Due to personal preference, tf.io.gfile is favored due to TensorFlow's C++ FileSystem API.


## Environment Variables

fileio caches variables upon first init to quickly restore its state, making managing things like Google's ADC and AWS's boto simpler.
Upon loading, fileio will set these vars, if present, to ensure other libs also see it. 

If you forget to get the variables, set 'RESET_FILEIO_ENV' = True to have it try to reload env vars.

## Cloud Storage Credentials

- GOOGLE_APPLICATION_CREDENTIALS (GCS)
- BOTO_PATH (AWS)
- AWS_ACCESS_KEY_ID (AWS)
- AWS_SECRET_ACCESS_KEY (AWS)

## Other Stuff

```python3

File.add_files(str, list) # will append new files to existing files (and deduplicate)
File.set_files(str, list) # will set new files and clear existing ones.

File.read_file(filename=None, binary=False, as_list=False, file_index=None)# will read the specified filename if provided, or will default to the next file_index (or file_index if provided). as_list will return a list of all items in the file. Otherwise, will return an iterator.

# Additionally, you can use the deterministic r/w filelike objects such as

from file_io import File
import json

f_in = 'gs://bucket/train_config.json'
f_out = '/path/train_config.json'

f = File()

data = json.load(f.infile(f_in))
json.dump(data, f.outfile(f_out, overwrite=True), indent=2)

import pandas as pd

csv_file = 'gs://bucket/mycsv.csv'

df = pd.read_csv(f.infile(csv_file, binary=True))

```

## Roadmap

- Support for SSH/SFTP/SCP
- Support for Azure
- Support for Compression
- Multi-file Writer Support/Sharding
- Better Deterministic binary mode for certain storages
