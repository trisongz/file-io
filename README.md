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

To be continued