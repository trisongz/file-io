import os
import abc
import collections

import pathlib
import types
import tempfile
import smart_open
import subprocess
import dill as pickle
import json

fileio_dir = os.path.abspath(os.path.dirname(__file__))

from file_io.config.env import get_env, get_cloud_clients
from file_io.utils.logger import get_logger
from tqdm.auto import tqdm

logger = get_logger()
env = get_env()
cloud = get_cloud_clients()

import tensorflow as tf
glob = tf.io.gfile.glob
gfile = tf.io.gfile

_require_modes = ['smartopen', 'tf', 'open']
_read = {
    'def': {
        'tf': 'r+',
        'so': 'r',
        'o': 'r'
    },
    'bin': {
        'tf': 'rb+',
        'so': 'rb',
        'o': 'rb'
    }
}
_write = {
    'def': {
        'tf': 'w+',
        'so': 'w',
        'o': 'w'
    },
    'bin': {
        'tf': 'wb+',
        'so': 'wb',
        'o': 'wb'
    }
}
_append = {
    'def': {
        'tf': 'a+',
        'so': 'a',
        'o': 'a'
    },
    'bin': {
        'tf': 'ab+',
        'so': 'ab',
        'o': 'ab'
    }
}



def get_file_ext(filename):
    return ''.join(pathlib.Path(filename).suffixes)

def build_input_files(filenames):
    _filenames = []
    if isinstance(filenames, str):
        if '*' in filenames:
            all_files = glob(filenames)
            _filenames.extend(all_files)
        elif tf.io.gfile.exists(filenames):
            _filenames.append(filenames)
    elif isinstance(filenames, list):
        for filename in filenames:
            if '*' in filename:
                all_files = glob(filename)
                _filenames.extend(all_files)
            elif tf.io.gfile.exists(filename):
                _filenames.append(filename)
    for filename in _filenames:
        if tf.io.gfile.isdir(filename):
            _filenames.remove(filename)
    return _filenames

def _file_exists(filename):
    try:
        return tf.io.gfile.exists(filename)
    except:
        return False

_ssh_exts = ['ssh', 'scp', 'sftp']

def get_read_fn(filename, binary=False, require=None):
    b = 'bin' if binary else 'def'
    if require:
        assert require in _require_modes, f'Required format should be in {_require_modes}'
        if require == 'smartopen':
            if filename.startswith('gs://'):
                return smart_open.open(filename, _read[b]['so'], transport_params=dict(client=cloud['gcs_client']))
            elif filename.startswith('s3://'):
                return smart_open.open(filename, _read[b]['so'], transport_params={'session': cloud['s3_client']})
            elif filename.startswith('azure://'):
                params = {'client': cloud['azure_client']} if cloud['azure_client'] else None
                return smart_open.open(filename, _read[b]['so'], transport_params=params)
            else:
                return smart_open.open(filename, _read[b]['so'])
        elif require == 'tf':
            return tf.io.gfile.GFile(filename, _read[b]['tf'])
        else:
            return open(filename, _read['o'])

    if filename.startswith('gs://'):
        return tf.io.gfile.GFile(filename, _read[b]['tf'])
        
    elif filename.startswith('s3://'):
        return smart_open.open(filename, _read[b]['so'], transport_params={'session': cloud['s3_client']})
    elif filename.startswith('https://') or filename.startswith('http://'):
        return smart_open.open(filename, _read[b]['so'])
    elif filename.startswith('hdfs://') or filename.startswith('webhdfs://'):
        return smart_open.open(filename, _read[b]['so'])
    else:
        return tf.io.gfile.GFile(filename, _read[b]['tf'])

def get_write_fn(filename, binary=False, overwrite=False, require=None):
    b = 'bin' if binary else 'def'
    f_exists = _file_exists(filename)
    if overwrite or not f_exists:
        _write_mode = _write
    else:
        _write_mode = _append
    if require:
        assert require in _require_modes, f'Required format should be in {_require_modes}'
        if require == 'smartopen':
            if filename.startswith('gs://'):
                return smart_open.open(filename, _write_mode[b]['so'], transport_params=dict(client=cloud['gcs_client']))
            elif filename.startswith('s3://'):
                return smart_open.open(filename, _write_mode[b]['so'], transport_params={'session': cloud['s3_client']})
            elif filename.startswith('azure://'):
                params = {'client': cloud['azure_client']} if cloud['azure_client'] else None
                return smart_open.open(filename, _write_mode[b]['so'], transport_params=params)
            else:
                return smart_open.open(filename, _write_mode[b]['so'])
        elif require == 'tf':
            return tf.io.gfile.GFile(filename, _write_mode[b]['tf'])
        else:
            return open(filename, _write_mode[b]['o'])

    if filename.startswith('gs://'):
        return tf.io.gfile.GFile(filename, _write_mode[b]['tf'])
    elif filename.startswith('s3://'):
        return smart_open.open(filename, _write_mode[b]['so'], transport_params={'session': cloud['s3_client']})
    elif filename.startswith('https://') or filename.startswith('http://'):
        return smart_open.open(filename, _write_mode[b]['so'])
    elif filename.startswith('hdfs://') or filename.startswith('webhdfs://'):
        return smart_open.open(filename, _write_mode[b]['so'])
    else:
        return tf.io.gfile.GFile(filename, _write_mode[b]['tf'])

_compressed_exts = ['.xz', '.zst', '.zip', '.gzip', '.bz2']
_non_local_prefixes = ['https://', 'gs://', 's3://', 'webhdfs://', 'sftp://']

def line_count(filename):
    file_ext = get_file_ext(filename)
    if file_ext in _compressed_exts:
        return 0
    for _prefix in _non_local_prefixes:
        if filename.startswith(_prefix):
            return 0
    return int(subprocess.check_output(['wc', '-l', filename]).split()[0])


def load_pkle(filename):
    with get_read_fn(filename) as f:
        return pickle.load(f)

def save_pkle(data, filename):
    with get_write_fn(filename) as f:
        pickle.dump(data, f)

def load_data(file_io):
    return pickle.load(file_io)

def save_data(data, file_io):
    pickle.dump(data, file_io)

def update_json(filename, update_dict):
    data = json.load(get_read_fn(filename))
    data.update(update_dict)
    json.dump(data, get_write_fn(filename, overwrite=True))
    return data

def read_json(filename):
    return json.load(get_read_fn(filename))

class File(object):
    def __init__(self, filenames=None, binary=False, progress=False):
        self.filenames = None
        self.num_files = 0
        if filenames:
            self.filenames = build_input_files(filenames)
            self.num_files = len(filenames)
        self.rb = binary
        self.use_tqdm, self.pbar = progress, None
        self.fn, self.read_func, self.write_func = None, None, None
        self.idx = 0
        self.file_idx = {}
    
    def set_reader(self, func):
        self.read_func = func
    
    def set_writer(self, func):
        self.write_func = func
    
    def open_writer(self, filename, binary=False, overwrite=False):
        self.close()
        self.fn = get_write_fn(filename, binary, overwrite)
        self.writeidx = 0
    
    def outfile(self, filename, binary=False, overwrite=False):
        return get_write_fn(filename, binary, overwrite)

    def infile(self, filename, binary=False):
        return get_read_fn(filename, binary)

    def write(self, x, newline='\n'):
        assert self.fn, 'No Writefile is open, use File.open_writer(filename)'
        if self.write_func:
            x = self.write_func(x)
        self.fn.write(x)
        if newline:
            self.fn.write(newline)
        self.writeidx += 1
        if self.writeidx % 500 == 0:
            self.fn.flush()

    def _iter_read(self, filename):
        idx = 0
        read_fn = get_read_fn(filename, self.rb)
        base_fn = filename.split('/')[-1].strip()
        pbar = tqdm(read_fn, desc=f"Reading [{base_fn}]", dynamic_ncols=True) if self.use_tqdm else None
        for item in read_fn:
            if self.read_func:
                item = self.read_func(item)
            yield item
            idx += 1
            if pbar:
                pbar.update()
        self.file_idx[filename] = {'read': idx}
        if pbar:
            pbar.close()

    def _list_read(self, filename):
        idx = 0
        results = []
        read_fn = get_read_fn(filename, self.rb)
        base_fn = filename.split('/')[-1].strip()
        pbar = tqdm(read_fn, desc=f"Reading [{base_fn}]", dynamic_ncols=True) if self.use_tqdm else None
        for item in read_fn:
            if self.read_func:
                item = self.read_func(item)
            results.append(item)
            idx += 1
            if pbar:
                pbar.update()

        self.file_idx[filename] = {'read': idx}
        if pbar:
            pbar.close()
        return results 

    def read_file(self, filename=None, binary=False, as_list=False, file_index=None):
        if binary:
            self.rb = True
        if file_index and not filename:
            assert isinstance(file_index, int), 'File Index needs to be an integer'
            assert file_index < (self.num_files - 1)
            filename = self.filenames[file_index]
        elif not filename:
            assert self.filenames, 'No Files have been added to read'
            filename = self.filenames[self.idx]
            self.idx += 1
        if as_list:
            return self._list_read(filename)
        return self._iter_read(filename)

    def close(self):
        if self.fn:
            try:
                filename = self.fn.filename
            except:
                filename = self.fn.name
            self.file_idx[filename] = {'write': self.writeidx}
            self.fn.flush()
            self.fn.close()

    def stats(self):
        return self.file_idx

    def set_files(self, filenames):
        self.filenames = build_input_files(filenames)
        self.num_files = len(filenames)

    def add_files(self, filenames):
        _filenames = build_input_files(filenames)
        self.num_files += len(_filenames)
        if self.filenames:
            _filenames = [f for f in _filenames if f not in self.filenames]
            self.filenames += _filenames
        else:
            self.filenames = _filenames

    def _iter_files(self):
        assert self.filenames, 'No Files have been added to read'
        for filename in self.filenames:
            return self.read_file(filename)

    def __call__(self, filenames=None):
        if filenames:
            self.filenames = build_input_files(filenames)
            self.num_files = len(filenames)
        return self._iter_files()

    def __iter__(self):
        return self._iter_files()

    def __len__(self):
        return self.num_files
    
    def __getitem__(self, x):
        return self.filenames[x]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def iterator_function(function=None, **kwargs):
    assert function is not None, "Please supply a function"
    def inner_func(function=function, **kwargs):
        generator = function(**kwargs)
        assert isinstance(generator, types.GeneratorType), "Invalid function"
        try:
            yield next(generator)
        except StopIteration:
            generator = function(**kwargs)
            yield next(generator)
    return inner_func

class LineSeekableFile:
    def __init__(self, seekable):
        self.fin = seekable
        self.line_map = list() # Map from line index -> file position.
        self.line_map.append(0)
        while seekable.readline():
            self.line_map.append(seekable.tell())

    def index(self):
        return self.line_map
    
    def __len__(self):
        return len(self.line_map)

    def __getitem__(self, index):
        # NOTE: This assumes that you're not reading the file sequentially.  
        # For that, just use 'for line in file'.
        self.fin.seek(self.line_map[index])
        return self.fin.readline()