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
File.jlg(filename)
File.jlw(data, filename, mode='auto', verbose=True)

# Multifile Readers

# .jsonl/.jsonlines
File.jgs(filenames)


```
### Upcoming Changes / APIs
- Support for setting JSON serializer [`simdjson` by default]
- Support for Google Sheets manipulation [`gspread`]
- Support for compressed files [`.zst`,  `.zip`, `.tar`, `.gz`, `.tar.gz`]

### Changelogs
July 7, 2021 - v0.1.14
- Remove Explicit need for Tensorflow in setup, but still require it at the moment.
    - This may help with macos Tensorflow installations using `tensorflow-macos`

July 2, 2021 - v0.1.13
- Change `.textread` to return string rather than list
    - `.textreadlines` replaces original function
- Update `.textlist` to support option for stripping newlines and have replacements
    - `strip_newlines = True`, will strip all newlines prior to return
    - `replacements: [ list | dict | str ] = None`, will iterate through and replace
- Update `.base(filename, with_ext=True)` to allow return without File Extension
- Add `.readfile` method to return `.read()` API
- Add `.mod_fname(filename, new_name=None, prefix=None, suffix=None, ext=None, directory=None, create_dirs=True, filename_only=False)`
    - `src = 'gs://mybucket/path/file.txt'`
    - `res = File.mod_fname(src, newname='newfile', ext='json', directory='/newdir', prefix='test_', suffix='_001')`
    - `>> res = /newdir/test_newfile_001.json`


June 30, 2021 - v0.1.11
- Added Dill as default pickler if installed
- Ability to set any pickle method that supports .dumps/.loads call with `File.set_pickler(name='pickler')` or `File.set_pickler(function=cloudpickle)`
- Hotfix to change method to dumps/loads
- Hotfix for .gsutil method which did not initialize properly.

June 11, 2021 - v0.1.8
- Hotfix for methods .split_file/.split_files

June 9, 2021 - v0.1.7
- Hotfix for Method .get_local
- Hotfix for method .jlgs

May 28, 2021 - v0.1.6
- Added Method to get User Dir
    - File.userdir

May 21, 2021 - v0.1.5
- Added TSV/CSV Write Methods
    - File.csvwrite
    - File.tsvwrite

May 20, 2021 - v0.1.4
- Hotfix for file.split_file(s) method to also return resulting filenames with `output_files` key

May 20, 2021 - v0.1.3
- Py Version Requirement Fix

May 19, 2021 - v0.1.2
- Minor Fixes
- Added Methods for Splitting Files/Items
    - File.calc_splits
    - File.split_items
    - File.split_file
    - File.split_files

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

