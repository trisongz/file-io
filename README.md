# file_io
 Deterministic File Lib to make working with Files across Object Storage easier


## Quickstart

```python3
!pip install --upgrade git+https://github.com/trisongz/file_io.git
!pip install --upgrade file-io


from fileio import File, Auth

# Auth object is for setting ADC if needed

Auth(adc='/path/to/adc.json')

'''
Recognized File Extensions

.json               - json
.jsonl/.jsonlines   - jsonlines
.csv                - csv
.tsv                - tsv with "\t" seperator
.txt                - txtlines
.pkl                - pickle
.pt                 - pytorch
.tfrecords          - tensorflow
'''

# Main auto classes
File.open(filename, mode='r', auto=True, device=None) # device is specific to pytorch. Set auto=False to get a barebones Posix via Gfile
File.save(data, filename, overwrite=False) # if not overwrite, will attempt to append for newline files
File.load(filenames, device=None) # yields generators per file, meaning you can have different file types
File.download(url, dirpath=None, filename=None, overwrite=False) # Downloads a single url
File.gdown(url, extract=True, verbose=False) # uses gdown lib to grab a google drive drive

# Main i/o classes (Not Binary)
File.read(filename) # 'r'
File.write(filename) # 'w'
File.append(filename) # 'a'

# Binary
File.wb(filename) # 'wb'
File.rb(filename) # 'rb'


# Batch downloaders
File.batch_download(urls, directory=None, overwrite=False) # downloads all urls into a directory, skipping if overwrite = True and exists
File.batch_gdown(urls, directory=None, extract=True, verbose=False) # downloads all gdrive urls to a directory

# Extension Specific 

# .json
File.jsonload(filename)
File.jsondump(dict, filename)

# .jsonl/.jsonlines (Single File)
File.jg(filename)
File.jlw(data, filename, mode='auto', verbose=True)

# Multifile Readers

# .jsonl/.jsonlines
File.jgs(filenames)


```

### Changelogs
May 12, 2021 - v0.1.1
- Minor Fixes
- Added Method
    - File.fmv

May 12, 2021 - v0.1.0
- Refactored Library
- Organized Methods
- Added MultiThreaded Wrapper
    - `from fileio import MultiThreadPipeline`
- Added gsutil wrapper method
    - File.gsutil
- Added Methods for Yaml
    - File.yload
    - File.yloads
    - File.ydump
    - File.ydumps
    - File.yparse
- Updated Methods for Json
    - File.jsonload
    - File.jsonloads
    - File.jsondump
    - File.jsondumps
    - File.jp
    - File.jwrite
    - File.jg
    - File.jgs
- Updated Methods for Jsonlines 
    - File.jll
    - File.jlp
    - File.jldumps
    - File.jlwrite
    - File.jlwrites
    - File.jlg
    - File.jlgs
    - File.jlload
    - File.jlw
    - File.jlsample
- Updated Methods for Text
    - File.textload
    - File.textwrite
    - File.textread
    - File.textlist
- Added Methods for Requests
    - File.rget
    - File.rpost
    - File.reqsess
- Added Methods for URL Encoding/Decoding
    - File.urlencode
    - File.urldecode
- Added Methods for Hashing
    - File.hash
    - File.checkhash
- Added Methods to Disable/Enable TQDM
    - File.enable_progress
    - File.disable_progress
- Added Utility Methods
    - File.cat
    - File.backup
    - File.findir
    - File.append_ext
    - File.copydir
    - File.dirglob
    - File.absdir
    - File.get_local
    - File.finalize
    - File.print
    - File.set_printer
- Fixed/Updated Methods
    - File.isfile
    - File.download
    - File.batch_download
    - File.pexists
    - File.whichpath
    - File.copy
    - File.bcopy
- Added TFDSIODataset

