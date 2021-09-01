# Provides compatibility to the old File API
import os
import csv
import requests
import random
import yaml
import hashlib
import tempfile
import math
import types
import gc

from itertools import accumulate
from datetime import datetime, timezone
from tqdm.auto import tqdm
from fileio import configs
from fileio.core import logger
from fileio.core.libs import (
    PICKLE_FUNC, 
    PT_DEVICE, 
    PT_FUNC,
    TF_FUNC,
    JSON_FUNC,
    JSON_PARSER
)
json = JSON_FUNC
jparser = JSON_PARSER
gfile = TF_FUNC.io.gfile

timestamp = lambda: datetime.now(timezone.utc).isoformat('T')
ftimestamp = lambda: datetime.now(timezone.utc).strftime("%b%d%Y_TM_%H%M%S")

_tmpdirs = []

class File(object):
    @classmethod
    def join(cls, path, *paths):
        return os.path.join(path, *paths)

    @classmethod
    def listfiles(cls, filepath):
        return gfile.listdir(filepath)

    @classmethod
    def isfile(cls, filepath):
        if not filepath.startswith('gs://'):
            return os.path.isfile(filepath)
        return (not gfile.isdir(filepath) and gfile.exists(filepath))

    @classmethod
    def listdir(cls, filepath):
        return gfile.listdir(filepath)
    
    @classmethod
    def curdir(cls):
        return os.getcwd()

    @classmethod
    def mkdir(cls, directory):
        return gfile.makedirs(directory)

    @classmethod
    def makedirs(cls, directory):
        return gfile.makedirs(directory)

    @classmethod
    def mkdirs(cls, directory):
        return gfile.makedirs(directory)
    
    @classmethod
    def userdir(cls, path=None, *paths, **kwargs):
        if not path:
            return os.path.expanduser("~")
        _dir = os.path.join(os.path.expanduser("~"), path, *paths)
        for k in ['mkdir', 'mkdirs', 'makedir', 'makedirs']:
            if kwargs.get(k): gfile.mkadirs(_dir)
        return _dir
    
    @classmethod
    def getdir(cls, filepath):
        if 'gs://' not in filepath:
            return os.path.abspath(os.path.dirname(filepath))
        fname = filepath.split('/')[-1]
        return filepath.replace(fname, '').strip()
    
    @classmethod
    def dirglob(cls, directory):
        if '*' in directory:
            return directory
        if directory.endswith('/'):
            return directory + '*'
        return directory + '/*'
    
    @classmethod
    def absdir(cls, directory):
        directory = File.getdir(directory)
        if not directory.endswith('/'):
            directory += '/'
        return directory

    @classmethod
    def isdir(cls, filepath):
        return gfile.isdir(filepath)

    @classmethod
    def glob(cls, filepath):
        return gfile.glob(filepath)

    @classmethod
    def mv(cls, src, dest, overwrite=False):
        return gfile.rename(src, dest, overwrite)
    
    @classmethod
    def fmv(cls, src, directory, overwrite=False):
        dest = File.join(directory, File.base(src))
        if not File.exists(dest) or overwrite:
            logger.info(f'Moving File {src} -> {dest}')
            File.mv(src, dest)
        else:    
            logger.error(f'Error: File Destination {dest} exists. Not Moving.')
        return dest


    @classmethod
    def rm(cls, filename):
        return gfile.remove(filename)
    
    @classmethod
    def rmdir(cls, filepath):
        return gfile.rmtree(filepath)

    @classmethod
    def copy(cls, src, dest, overwrite=True):
        try:
            return gfile.copy(src, dest, overwrite)
        except Exception as e:
            logger.error(f'Error in Copying {src} -> {dest}: {str(e)}')
            return None
    
    @classmethod
    def bcopy(cls, src, directory, overwrite=True, verbose=False):
        if not gfile.exists(directory):
            gfile.makedirs(directory)
        dest = os.path.join(directory, os.path.basename(src))
        if not gfile.exists(dest) or overwrite:
            if verbose:
                logger(f'Copying {src} -> {dest}')
            gfile.copy(src, dest, overwrite)
        elif verbose:
            logger(f'Skipping {src} -> {dest} Exists')
        return dest
    
    # Does not handle recursive
    @classmethod
    def copydir(cls, src_dir, dest_dir, overwrite=True, gsutil=False):
        if gsutil:
            cmd = f'gsutil -m cp -r {File.dirglob(src_dir)} {File.absdir(dest_dir)}'
            return File.gsutil(cmd)
        File.mkdirs(dest_dir)
        filenames = File.glob(File.dirglob(src_dir))
        return [File.bcopy(fname, dest_dir, overwrite, verbose=True) for fname in filenames]

    @classmethod
    def append_ext(cls, filepath, append_key, directory=None, fext=None, abs=True):
        filename = File.base(filepath)
        afilename = filename.replace('.', f'_{append_key}.')
        if abs:
            directory = File.getdir(filepath)
        if fext:
            afilename = afilename.rsplit('.', 1)[0] + f'.{fext}'
        if directory:
            return File.join(directory, afilename)
        return afilename
    
    @classmethod
    def change_ext(cls, filepath, ext, directory=None):
        filename = File.base(filepath)
        ext_file = filename.split('.', 1)[0] + '.' + ext
        if directory:
            return File.join(directory, ext_file)
        return ext_file

    @classmethod
    def mod_fname(cls, filename, newname=None, prefix=None, suffix=None, ext=None, directory=None, create_dirs=True, filename_only=False, space_replace=None):
        basefname = File.base(filename)
        basesplit = basefname.split('.', 1)
        basenoext = basesplit[0]
        fname = basenoext if not newname else newname
        if ext:
            if '.' not in ext: ext = '.' + ext
            if ext in fname: fname = fname.replace(ext, '')
        elif newname and '.' in newname:
            ext = File.ext(newname)
            fname = newname.replace(ext, '')
        else:
            ext = File.ext(filename)
        if prefix: fname = prefix + fname
        if suffix: fname += suffix
        if space_replace: fname = fname.replace(' ', space_replace).strip()
        fullname = fname + ext
        if filename_only:
            return fullname
        directory = directory or File.getdir(filename)
        if create_dirs:
            File.makedirs(directory)
        return File.join(directory, fullname)


    @classmethod
    def findir(cls, filepath, directory):
        filename = File.base(filepath)
        return File.pexists(directory, filename)

    @classmethod
    def backup(cls, filepath, directory=None, overwrite=False, key='bk'):
        if (
            directory
            and not key
            and overwrite
            and File.findir(filepath, directory)
        ):
            return File.bcopy(filepath, directory, overwrite)

        append_key = 'bk' if key and key == 'bk' else ftimestamp()
        if not directory:
            directory = File.getdir(filepath)
        File.mkdirs(directory)
        new_fname = File.append_ext(filepath, append_key, directory)
        File.copy(filepath, new_fname)
        return new_fname

    @classmethod
    def exists(cls, filepath):
        return gfile.exists(filepath)
    
    @classmethod
    def pexists(cls, path, *paths):
        return gfile.exists(File.join(path, *paths))
    
    @classmethod
    def whichpath(cls, path1, path2):
        if gfile.exists(path1):
            return path1
        if gfile.exists(path2):
            return path2
        raise ValueError

    @classmethod
    def base(cls, filepath, with_ext=True):
        f = os.path.basename(filepath)
        if with_ext:
            return f
        _, e = os.path.splitext(f)
        return f.replace(e, '')

    @classmethod
    def ext(cls, filepath):
        f = os.path.basename(filepath)
        _, e = os.path.splitext(f)
        return e
    
    @classmethod
    def cat(cls, filepath, verbose=True):
        if gfile.exists(filepath):
            f = gfile.GFile(filepath, 'r')
            text = f.readlines()
            if verbose:
                print(text)
            return text
        return None    

    @classmethod
    def touch(cls, filepath, overwrite=False):
        if not gfile.exists or overwrite:
            with gfile.GFile(filepath, 'w') as f:
                f.write('\n')
                f.flush()
            f.close()

    # File R/W/A Methods
    @classmethod
    def append(cls, filename, mode='a'):
        return gfile.GFile(filename, mode)
    
    @classmethod
    def read(cls, filename, mode='r'):
        return gfile.GFile(filename, mode)
    
    
    @classmethod
    def write(cls, filename, mode='w'):
        return gfile.GFile(filename, mode)

    @classmethod
    def rb(cls, filename):
        return gfile.GFile(filename, 'rb')
    
    @classmethod
    def wb(cls, filename):
        return gfile.GFile(filename, 'wb')

    @classmethod
    def readlines(cls, filename):
        with gfile.GFile(filename, 'r') as f:
            return f.readlines()
    
    @classmethod
    def readfile(cls, filename, mode='r'):
        with gfile.GFile(filename, mode) as f:
            return f.read()

    @classmethod
    def open(cls, filename, mode='r', auto=True, device=None, **kwargs):
        if 'r' in mode and auto:
            if filename.endswith('.pkl'):
                return File.pload(filename)
            
            if filename.endswith('.jsonl') or filename.endswith('.jsonlines'):
                return File.jg(filename)
            
            if filename.endswith('.json'):
                return File.jsonload(filename)
            
            if filename.endswith('.pt'):
                return File.ptload(filename, device)

        return gfile.GFile(filename, mode)
    
    @classmethod
    def save(cls, data, filename, overwrite=False):
        if filename.endswith('.pkl'):
            return File.pklsave(data, filename)
        
        if filename.endswith('.jsonl') or filename.endswith('.jsonlines'):
            return File.jlw(data, filename)
        
        if filename.endswith('.json'):
            return File.jsondump(data, filename)
        
        if filename.endswith('.pt') or filename.endswith('.pb'):
            return File.ptsave(data, filename)
        
        if filename.endswith('.txt'):
            return File.textwrite(data, filename, overwrite)
        
        logger.info('Unrecognized Extension. Not Saving')
        return
    
    @classmethod
    def load(cls, filenames, device=None):
        filenames = File.fsorter(filenames)
        _is_tfr = bool(sum(1 for fn in filenames if (fn.endswith('.tfrecords') or fn.endswith('.tfrecord'))) > 1)
        if _is_tfr:
            return File.tfreader(filenames)
        for filename in filenames:
            if filename.endswith('.pkl'):
                yield File.pload(filename)
            
            elif filename.endswith('.jsonl') or filename.endswith('.jsonlines'):
                iterator = File.jlg(filename)
                yield from iterator
            
            elif filename.endswith('.json'):
                yield File.jsonload(filename)
            
            elif filename.endswith('.pt') or filename.endswith('.pb'):
                yield File.ptload(filename, device)
            
            elif filename.endswith('.txt'):
                iterator = File.textload(filename)
                yield from iterator
            
            elif filename.endswith('.csv'):
                try:
                    yield File.csvdictload(filename)
                except:
                    yield File.csvload(filename)
            
            elif filename.endswith('.tsv'):
                try:
                    yield File.tsvdictload(filename)
                except:
                    yield File.tsvload(filename)
            
            else:
                logger.info(f'Unrecognized File Extension: {filename}')
        return

    
    @classmethod
    def writer(cls, filename, mode='w', auto=True, overwrite=False):
        if filename.endswith('.tfrecords'):
            return File.tfwriter(filename)
        if auto:
            mode = File.writemode(filename, overwrite)
        return gfile.GFile(filename, mode)


    @classmethod
    def flush(cls, f):
        f.flush()

    @classmethod
    def fclose(cls, f):
        f.flush()
        f.close()
    
    @classmethod
    def writemode(cls, filepath, overwrite=False):
        if gfile.exists(filepath):
            return 'a'
        return 'w'

    @classmethod
    def autowrite(cls, filename, overwrite=False):
        if overwrite or not gfile.exists(filename):
            return 'w'
        return 'a'

    # Json Methods
    @classmethod
    def jsonload(cls, filename):
        return json.load(gfile.GFile(filename, 'r'))
    
    @classmethod
    def jsonloads(cls, string):
        return json.loads(string)
    
    @classmethod
    def jsondump(cls, obj, filename, indent=2, ensure_ascii=False):
        return json.dump(obj, gfile.GFile(filename, 'w'), indent=indent, ensure_ascii=ensure_ascii)
    
    @classmethod
    def jsondumps(cls, pdict, ensure_ascii=False):
        return json.dumps(pdict, ensure_ascii=ensure_ascii)
    
    @classmethod
    def jp(cls, line):
        return jparser.parse(line).as_dict()
    

    @classmethod
    def jwrite(cls, data, filename, mode='auto'):
        mode = mode if mode != 'auto' else File.autowrite(filename)
        with gfile.GFile(filename, mode=mode) as f:
            File.jldump(data, f)
        File.fclose(f)
    
    @classmethod
    def jg(cls, filename, handle_errors=True):
        try:
            return File.jsonload(filename)
        except Exception as e:
            if not handle_errors:
                logger.log(f'Error parsing File: {str(e)}')
                raise e
            return None

    @classmethod
    def jgs(cls, filenames, handle_errors=True):
        filenames = File.fsorter(filenames)
        for fname in filenames:
            yield File.jg(fname, handle_errors)

    # Json Lines Methods
    @classmethod
    def jll(cls, line):
        return json.loads(line)
    
    @classmethod
    def jlp(cls, line):
        try:
            return File.jp(line)
        except:
            return File.jll(line)

    @classmethod
    def jldumps(cls, data, f):
        f.write(json.dumps(data, ensure_ascii=False))
        f.write('\n')
    
    @classmethod
    def jlwrite(cls, data_items, filename, mode='auto'):
        mode = mode if mode != 'auto' else File.autowrite(filename)
        with gfile.GFile(filename, mode=mode) as f:
            for data in data_items:
                File.jldumps(data, f)
        File.fclose(f)
    
    @classmethod
    def jlwrites(cls, data_items, filename, mode='auto'):
        return File.jlwrite(data_items, filename, mode)

    @classmethod
    def jwriteauto(cls, data, filename, mode='auto'):
        if isinstance(data, list):
            return File.jlwrite(data, filename, mode)
        return File.jwrite(data, filename, mode)

    @classmethod
    def jlg(cls, filename, handle_errors=True):
        with gfile.GFile(filename, 'r') as f:
            for l in f:
                try:
                    yield File.jlp(l)
                except StopIteration:
                    break
                except Exception as e:
                    if not handle_errors:
                        logger.log(f'Error parsing File: {str(e)}')
                        raise e
                
    
    @classmethod
    def jlgs(cls, filenames, handle_errors=True):
        filenames = File.fsorter(filenames)
        for fname in filenames:
            yield from File.jlg(fname, handle_errors)
    

    @classmethod
    def jlload(cls, filename, as_iter=False, index=False, handle_errors=True):
        if as_iter:
            if not index:
                return File.jlg(filename, handle_errors=handle_errors)
            yield from enumerate(File.jlg(filename, handle_errors=handle_errors))

        else:
            if index:
                return {x: item for x, item in enumerate(File.jlg(filename, handle_errors=handle_errors))}
            return [item for item in File.jlg(filename, handle_errors=handle_errors)]


    @classmethod
    def jlw(cls, data, filename, mode='auto', verbose=True):
        mode = mode if mode != 'auto' else File.autowrite(filename)
        if isinstance(data, dict):
            return File.jlwrite(data, filename, mode=mode)
        _good, _bad, failed = 0, 0, []
        with gfile.GFile(filename, mode) as f:
            for x, d in enumerate(data):
                try:
                    File.jldumps(d, f)
                    _good += 1
                except StopIteration:
                    break
                except Exception as e:
                    if verbose:
                        logger.info(f'Error: {str(e)} Writing Line {d}')
                    failed.append({'idx': x, 'data': d, 'error': str(e)})
                    _bad += 1
            File.fclose(f)
        logger.info(f'Wrote {_good}/{_good + _bad} Lines [Mode: {mode}] - Failed: {_bad}')
        return failed

    @classmethod
    def jlsample(cls, filenames, num_samples=50, shuffle=True, printer=None):
        if printer:
            File.set_printer(printer)
        filenames = File.fsorter(filenames)
        samples = []
        for fname in filenames:
            reader = LineSeekableFile(gfile.GFile(fname, 'r'))
            File.print(f'Sampling {num_samples} items from {fname}')
            if shuffle:
                fidxs = [random.randint(0, len(reader)) for i in range(num_samples)]
            else:
                fidxs = [i for i in enumerate(range(num_samples))]
            for idx in fidxs:
                ex = File.jll(reader[idx])
                File.print(f'{idx} -> {ex}')
                samples.append(ex)
        return samples

    # Pickle Methods
    @classmethod
    def pklsave(cls, obj, filename, **kwargs):
        data = PICKLE_FUNC.dumps(obj, **kwargs)
        with gfile.GFile(filename, 'wb') as f:
            f.write(data)
            f.flush()

    @classmethod
    def pklload(cls, filename, **kwargs):
        with gfile.GFile(filename, 'rb') as f:
            data = PICKLE_FUNC.loads(f.read(), **kwargs)
        return data
    
    @classmethod
    def pload(cls, filename, **kwargs):
        return File.pklload(filename)
    
    @classmethod
    def psave(cls, filename, **kwargs):
        return File.pklsave(filename, **kwargs)

    # Torch Methods
    @classmethod
    def torchsave(cls, obj, filename):
        assert PT_FUNC, 'pytorch is not available'
        return PT_FUNC.save(obj, gfile.GFile(filename, 'wb'))

    @classmethod
    def torchload(cls, filename, device=None):
        return PT_FUNC.load(gfile.GFile(filename, 'rb'), map_location=device or PT_DEVICE)
    
    @classmethod
    def ptsave(cls, obj, filename):
        return File.torchsave(obj, filename)
    
    @classmethod
    def ptload(cls, filename, device=None):
        return File.torchload(filename, device)
    
    # CSV / TSV Methods
    @classmethod
    def csvload(cls, filename):
        return list(csv.reader(gfile.GFile(filename, 'r')))

    @classmethod
    def tsvload(cls, filename):
        return list(csv.reader(gfile.GFile(filename, 'r'), delimiter='\t'))
    
    @classmethod
    def csvdictload(cls, filename):
        return dict(csv.DictReader(gfile.GFile(filename, 'r')))

    @classmethod
    def tsvdictload(cls, filename):
        return dict(csv.DictReader(gfile.GFile(filename, 'r'), delimiter='\t'))

    @classmethod
    def csvreader(cls, f):
        return csv.DictReader(f)
    
    @classmethod
    def tsvreader(cls, f):
        return csv.DictReader(f, delimiter='\t')
    
    @classmethod
    def csvwrite(cls, data, filename, mode='auto', keys=None, delimiter=','):
        mode = mode if mode != 'auto' else File.autowrite(filename)
        _is_dict = False
        if isinstance(data, dict):
            keys = keys or list(data.keys())
            _is_dict = True
        
        elif isinstance(data, list):
            if isinstance(data[0], dict):
                _is_dict = True
                keys = keys or list(data[0].keys())

        with gfile.GFile(filename, mode) as f:
            writer = csv.DictWriter(f, keys, delimiter=delimiter) if _is_dict else csv.writer(f, delimiter=delimiter)
            if keys and mode != 'a':
                writer.writeheader()
            writer.writerows(data)
    
    @classmethod
    def tsvwrite(cls, data, filename, mode='auto', keys=None):
        return File.csvwrite(data, filename, mode, keys, delimiter='\t')

    # Text Lines Methods
    @classmethod
    def textload(cls, filename):
        with gfile.GFile(filename, 'r') as f:
            for line in f:
                yield line.strip()

    @classmethod
    def textwrite(cls, data, filename, overwrite=False):
        mode = 'w' if overwrite or not gfile.exists(filename) else 'a'
        with gfile.GFile(filename, mode) as f:
            if isinstance(data, list):
                for d in data:
                    f.write(d + '\n')
            else:
                f.write(data + '\n')
            f.flush()

    @classmethod
    def textread(cls, filename):
        return File.readfile(filename)

    @classmethod
    def textreadlines(cls, filename):
        return File.readlines(filename)

    @classmethod
    def textlist(cls, filename, strip_newlines=True, replacements=None):
        items = File.readlines(filename)
        if strip_newlines:
            items = [i.strip() for i in items]
        if replacements:
            if isinstance(replacements, list):
                logger.info('Assuming Replacements are ""')
                for r in replacements:
                    items = [i.replace(r, '') for i in items]
            elif isinstance(replacements, dict):
                logger.info('Replacing all items with key = val')
                for k,v in replacements.items():
                    items = [i.replace(k, v) for i in items]
            elif isinstance(replacements, str):
                logger.info(f'Replacing {replacements} = ""')
                items = [i.replace(replacements, '') for i in items]
            else:
                raise ValueError
        return items

    @classmethod
    def nlwrite(cls, data, f):
        f.write(data + '\n')

    
    # TF Data Methods
    @classmethod
    def tfeager(cls, enable=True):
        if enable:
            TF_FUNC.compat.v1.enable_eager_execution()
        else:
            TF_FUNC.compat.v1.disable_v2_behavior()
    
    @classmethod
    def tflines(cls, filenames):
        File.tfeager()
        fnames = File.fsorter(filenames)
        return TF_FUNC.data.TextLineDataset(fnames, num_parallel_reads=TF_FUNC.data.experimental.AUTOTUNE)
    
    @classmethod
    def tfjl(cls, filenames, handle_errors=True, verbose=False):
        pipeline = File.tflines(filenames)
        for idx, x in enumerate(pipeline.as_numpy_iterator()):
            if not handle_errors:
                yield File.jlp(x)
            else:
                try:
                    yield File.jlp(x)
                except Exception as e:
                    if verbose:
                        logger.info(f'Error on {idx}: {str(e)} - {x}')

    @classmethod
    def tfcsv(cls, filenames):
        for f in File.gfiles(filenames):
            reader = File.csvreader(f)
            yield from reader

    @classmethod
    def tftl(cls, filenames, handle_errors=True, verbose=True):
        pipeline = File.tflines(filenames)
        for idx, x in enumerate(pipeline.as_numpy_iterator()):
            if not handle_errors:
                yield x.strip()
            else:
                try:
                    yield x.strip()
                except StopIteration:
                    break
                except Exception as e:
                    if verbose:
                        logger.info(f'Error on {idx}: {str(e)} - {x}')
    
    @classmethod
    def tfreader(cls, filenames, compression=None, buffer=None, num_parallel=TF_FUNC.data.experimental.AUTOTUNE):
        return TF_FUNC.data.TFRecordDataset(filenames, compression_type=compression, buffer_size=buffer, num_parallel_reads=num_parallel)
    
    @classmethod
    def tfwriter(cls, filename):
        return TF_FUNC.io.TFRecordWriter(filename)
    
    # YAML Methods
    @classmethod
    def ydump(cls, data, filepath):
        return yaml.dump(data, stream=gfile.GFile(filepath, 'w'), indent=2)
    
    @classmethod
    def ydumps(cls, data):
        return yaml.dump(data)
    
    @classmethod
    def yloads(cls, data):
        return yaml.load(data)

    @classmethod
    def yload(cls, filename):
        return File.yloads(gfile.GFile(filename, 'r'))
    
    @classmethod
    def yparse(cls, data_or_file):
        if File.isfile(data_or_file):
            return yaml.parse(gfile.GFile(data_or_file, 'r'))
        return yaml.parse(data_or_file)
    
    # Download Methods
    @classmethod
    def download(cls, url, dirpath=None, filename=None, overwrite=False, quiet=True, chunk_size=1024):
        if not filename:
            filename = File.base(url)
        if dirpath:
            File.join(dirpath, filename)
        if File.exists(filename) and not overwrite:
            logger.info(f'{filename} exists and overwrite = False')
            return
        rstream = requests.get(url, stream=True)
        with File.wb(filename) as f:
            for chunk in tqdm(rstream.iter_content(chunk_size=chunk_size), desc=f'Downloading {filename}', disable=(quiet or not configs.ENABLE_PROGRESS_BAR)):
                if not chunk:
                    break
                f.write(chunk)
            f.flush()
        f.close()
    
    @classmethod
    def absdownload(cls, url, filepath, overwrite=False, quiet=True, chunk_size=1024):
        if File.exists(filepath) and not overwrite:
            logger.info(f'{filepath} exists and overwrite = False')
            return
        rstream = requests.get(url, stream=True)
        with File.wb(filepath) as f:
            for chunk in tqdm(rstream.iter_content(chunk_size=chunk_size), desc=f'Downloading {filepath}', disable=(quiet or not configs.ENABLE_PROGRESS_BAR)):
                if not chunk:
                    break
                f.write(chunk)
            f.flush()
        f.close()

    @classmethod
    def batch_download(cls, urls, directory=None, overwrite=False):
        if not directory:
            directory = os.getcwd()
            logger.info(f'No Directory Set. Using: {directory}')
        logger.info(f'Downloading {len(urls)} Urls')
        for url in urls:
            try:
                File.download(url, dirpath=directory, overwrite=overwrite)
            except Exception as e:
                logger.info(f'Failed to download {url}: {str(e)}')
    
    @classmethod
    def gurl(cls, url_or_id):
        base_url = 'https://drive.google.com/uc?id='
        if 'drive.google.com' in url_or_id:
            url_or_id = url_or_id.split('https://drive.google.com/file/d/')[-1].split('/view?usp=sharing')[0]
        return base_url + url_or_id

    @classmethod
    def gdown(cls, url, extract=True, verbose=False):
        import gdown as _gdown
        url = File.gurl(url)
        if extract:
            return _gdown.cached_download(url, postprocess=_gdown.extractall, quiet=verbose)
        return _gdown.download(url, quiet=verbose)

    @classmethod
    def batch_gdown(cls, urls, directory=None, extract=True, verbose=False):
        if directory:
            os.chdir(directory)
            logger.info(f'Downloading into: {directory}')
        else:
            logger.info(f'No directory set. Using: {os.getcwd()}')
        logger.info(f'Downloading {len(urls)} Urls')
        for url in urls:
            try:
                File.gdown(url, extract, verbose)
            except Exception as e:
                logger.info(f'Failed to download {url}: {str(e)}')

    # Web Utils
    @classmethod
    def reqsess(cls, headers=None, cookies=None):
        s = requests.Session()
        if headers:
            s.headers.update(headers)
        if cookies:
            if isinstance(cookies, dict):
                cookies = requests.utils.cookiejar_from_dict(cookies)
            s.cookies = cookies
        return s
    
    @classmethod
    def urlencode(cls, url):
        return requests.utils.quote(url)
    
    @classmethod
    def urldecode(cls, url):
        return requests.utils.unquote(url)

    @classmethod
    def getreq(cls, url, method, headers=None, params=None, data=None, json_data=None, auth=None, cookies=None, filepath=None):
        assert method.upper() in ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD']
        rparams = {'url': requests.utils.quote(url)}
        if cookies:
            if isinstance(cookies, dict):
                cookies = requests.utils.cookiejar_from_dict(cookies)
            rparams['cookies'] = cookies
        if headers:
            rparams['headers'] = headers
        if params:
            rparams['params'] = params
        if auth:
            rparams['auth'] = auth
        if json_data:
            rparams['json'] = json_data
        if filepath:
            rparams['files'] = (File.base(filepath), File.rb(filepath))
        return requests.request(method.upper(), **rparams)
        
    @classmethod
    def rget(cls, url, headers=None, params=None, data=None, json_data=None, auth=None, cookies=None):
        return File.getreq(url, 'GET', headers, params, data, json_data, auth, cookies)

    @classmethod
    def rpost(cls, url, headers=None, params=None, data=None, json_data=None, auth=None, cookies=None, filepath=None):
        return File.getreq(url, 'POST', headers, params, data, json_data, auth, cookies, filepath)

    # Utilities
    @classmethod
    def fsorter(cls, filenames):
        fnames = []
        if isinstance(filenames, str) or not isinstance(filenames, list):
            filenames = [filenames]
        for fn in filenames:
            if not isinstance(fn, str):
                fn = str(fn)
            if fn.endswith('*'):
                _newfns = gfile.glob(fn)
                _newfns = [f for f in _newfns if not gfile.isdir(f) and gfile.exists(f)]
                fnames.extend(_newfns)
            elif not gfile.isdir(fn) and gfile.exists(fn):
                fnames.append(fn)
        return fnames

    @classmethod
    def gfile(cls, filename, mode):
        return gfile.GFile(filename, mode)
    
    @classmethod
    def gfiles(cls, filenames, mode='r'):
        fnames = File.fsorter(filenames)
        for fn in fnames:
            yield gfile.GFile(fn, mode)

    @classmethod
    def num_lines(cls, filenames):
        pipeline = File.tflines(filenames)
        return sum(1 for _ in tqdm(pipeline, desc='Getting Total Lines..'))

    @property
    def root(self):
        return os.path.abspath(os.path.dirname(__file__))
    
    @classmethod
    def get_root(cls, path=None):
        if not path:
            return File.root
        return os.path.abspath(os.path.dirname(path))
    
    @classmethod
    def is_cloud(cls, filename):
        if filename.startswith('gs://'):
            return (True, 'gcs')
        if filename.startswith('s3://'):
            return (True, 's3')
        else:
            return (False, None)
    

    @classmethod
    def mktmp(cls, filename, overwrite=True):
        global _tmpdirs
        if _tmpdirs:
            tmp_dir = _tmpdirs[-1]
        else:
            tmp_dir = tempfile.TemporaryDirectory()
            _tmpdirs.append(tmp_dir)
        return File.bcopy(filename, tmp_dir, overwrite)

    @classmethod
    def rmtmp(cls, mode='all'):
        global _tmpdirs
        _tmpmethods = ['all', 'first', 'last']
        assert mode in _tmpmethods, 'Not available mode in [all, first, last]'
        if mode == 'all':
            for tmpdir in _tmpdirs:
                tmpdir.cleanup()
                #rmdir(tmpdir)
            _tmpdirs = []
        else:
            pos = -1 if mode == 'last' else 0
            tmpdir = _tmpdirs.pop(pos)
            tmpdir.cleanup()

    @classmethod
    def hash(cls, text):
        return hashlib.sha256(str.encode(text)).hexdigest()
    
    @classmethod
    def checkhash(cls, input_key, hashed_key):
        return bool(File.hash(input_key) == hashed_key)
    
    
    @classmethod
    def enable_progress(cls):
        logger.info(f'Enabling TQDM. Currently Enabled: {configs.ENABLE_PROGRESS_BAR}')
        configs.ENABLE_PROGRESS_BAR = True
    
    @classmethod
    def enable_pbar(cls):
        return File.enable_progress()
    
    @classmethod
    def disable_progress(cls):
        logger.info(f'Disabling TQDM. Currently Enabled: {configs.ENABLE_PROGRESS_BAR}')
        configs.ENABLE_PROGRESS_BAR = False
    
    @classmethod
    def disable_pbar(cls):
        return File.disable_progress()
    
    @classmethod
    def finalize(cls, src_dict, dest_dict, keys=None, overwrite=True):
        keys = keys or list(src_dict.keys())
        for key in keys:
            logger.info(f'Copying {src_dict[key]} -> {dest_dict[key]}')
            File.copy(src_dict[key], dest_dict[key], overwrite=overwrite)
    
    @classmethod
    def get_local(cls, filenames, directory=None, overwrite=False):
        if not directory:
            directory = File.join(os.getcwd(), 'data')
        filenames = File.fsorter(filenames)
        lpaths = []
        for fpath in filenames:
            lpath = File.bcopy(fpath, directory, overwrite=overwrite)
            lpaths.append(lpath)
        return lpaths
    
    @classmethod
    def calc_splits(cls, num_items, split_dict):
        if sum(list(split_dict.values())) > 1.0:
            res = {f'{k}_items': math.ceil(num_items / v) for k,v in split_dict.items()}        
        else:
            assert sum(list(split_dict.values())) <= 1.0
            res = {f'{k}_items': math.ceil(num_items * v) for k,v in split_dict.items()}
        while sum(list(res.values())) > num_items:
            for k,v in res.items():
                res[k] -= 1
                if sum(list(res.values())) <= num_items:
                    break
        return res

    @classmethod
    def split_items(cls, item_list, split_dict={'train': 0.85, 'val': 0.10, 'test': 0.05}, shuffle=True):
        split_sizes = File.calc_splits(len(item_list), split_dict)
        if shuffle:
            logger.info(f'Shuffling Data')
            random.shuffle(item_list)
        split_lens = list(split_sizes.values())
        data = [item_list[x - y: x] for x, y in zip(accumulate(split_lens), split_lens)]
        total_split = sum(len(x) for x in data)
        data = {k: data[x] for x, k in enumerate(split_dict) if len(data[x]) == split_sizes[f'{k}_items']}
        return {'data': data, 'total_items': len(item_list), 'total_split_items': total_split, 'split_dict': split_dict, 'split_lengths': split_lens, 'split_sizes': split_sizes, 'shuffled': shuffle}
    
    @classmethod
    def lazy_split_items(cls, lazy_iterator, split_dict={'train': 0.85, 'val': 0.10, 'test': 0.05}, shuffle=True):
        split_sizes = File.calc_splits(len(lazy_iterator), split_dict)
        item_idx = lazy_iterator.get_index()
        if shuffle:
            logger.info(f'Shuffling Data')
            random.shuffle(item_idx)
        split_lens = list(split_sizes.values())
        data_idx = [item_idx[x - y: x] for x, y in zip(accumulate(split_lens), split_lens)]
        total_split = sum(len(x) for x in data_idx)
        data = []
        for idx in data_idx:
            items = [lazy_iterator[i] for i in idx]
            data.append(items)
        data = {k: data[x] for x, k in enumerate(split_dict) if len(data[x]) == split_sizes[f'{k}_items']}
        return {'data': data, 'total_items': len(lazy_iterator), 'total_split_items': total_split, 'split_dict': split_dict, 'split_lengths': split_lens, 'split_sizes': split_sizes, 'shuffled': shuffle}

    @classmethod
    def lazy_split_file(cls, filename, split_dict={'train': 0.85, 'val': 0.10, 'test': 0.05}, output_format='jsonl', directory=None, shuffle=True):
        logger.info('Using Lazy Iterator')
        lazy_iterator = LineSeekableFile(File.read(filename))
        logger.info(f'Lazy Iterator Items: {len(lazy_iterator)}')
        out_fns = {k: File.append_ext(filename, k, directory=directory, abs=bool(not directory)) for k in list(split_dict.keys())}
        out_fns['results'] = File.append_ext(filename, 'results', directory=directory, fext='json', abs=bool(not directory))
        split_sizes = File.calc_splits(len(lazy_iterator), split_dict)
        item_idx = lazy_iterator.get_index()
        if shuffle:
            logger.info(f'Shuffling Data')
            random.shuffle(item_idx)
        split_lens = list(split_sizes.values())
        data_idx = [item_idx[x - y: x] for x, y in zip(accumulate(split_lens), split_lens)]
        total_split = sum(len(x) for x in data_idx)
        data_idx = {k: data_idx[x] for x, k in enumerate(split_dict) if len(data_idx[x]) == split_sizes[f'{k}_items']}
        split_data = {'total_items': len(lazy_iterator), 'total_split_items': total_split, 'split_dict': split_dict, 'split_lengths': split_lens, 'split_sizes': split_sizes, 'shuffled': shuffle}
        logger.info(f'Split Sizes for {filename}: {split_data["split_sizes"]}.\nOutput Files: {out_fns}')

        res_meta = {'filename': filename, 'output_files': out_fns}
        res_meta.update(split_data)
        for split_key in split_dict:
            items = [lazy_iterator[i] for i in data_idx[split_key]]
            if output_format in ['jsonl', 'jsonlines', 'jl', 'jlines']:
                File.jlwrites(items, out_fns[split_key], mode='w')
            
            else:
                logger.error(f'Format {output_format} is not supported')
                assert ValueError
            del items
            gc.collect()
            
        logger.info(f'Final Metadata: {res_meta}')
        File.jsondump(res_meta, out_fns['results'])
        return res_meta


    @classmethod
    def split_file(cls, filename, split_dict={'train': 0.85, 'val': 0.10, 'test': 0.05}, output_format='jsonl', directory=None, shuffle=True):
        iterator = File.jlg(filename)
        items = [ex for ex in iterator]
        out_fns = {k: File.append_ext(filename, k, directory=directory, abs=bool(not directory)) for k in list(split_dict.keys())}
        out_fns['results'] = File.append_ext(filename, 'results', directory=directory, fext='json', abs=bool(not directory))
        split_data = File.split_items(items, split_dict, shuffle)

        logger.info(f'Split Sizes for {filename}: {split_data["split_sizes"]}.\nOutput Files: {out_fns}')
        data = split_data.pop('data')

        res_meta = {'filename': filename, 'output_files': out_fns}
        res_meta.update(split_data)
        for split_key in split_dict:
            if output_format in ['jsonl', 'jsonlines', 'jl', 'jlines']:
                File.jlwrites(data[split_key], out_fns[split_key], mode='w')
            
            else:
                logger.error(f'Format {output_format} is not supported')
                assert ValueError
            
        logger.info(f'Final Metadata: {res_meta}')
        File.jsondump(res_meta, out_fns['results'])
        return res_meta

    @classmethod
    def split_files(cls, filenames, split_dict={'train': 0.85, 'val': 0.15, 'test': 0.05}, output_format='jsonl', merge_files=False, dataset_name=None, directory=None, shuffle=True):
        if not merge_files:
            for fname in filenames:
                yield File.split_file(fname, split_dict=split_dict, output_format=output_format, directory=directory, shuffle=shuffle)
            return
        assert dataset_name, 'Dataset must have a name'
        assert directory, 'Directory must be set'
        assert output_format == 'jsonl', 'Format must be Jsonl'
        items = [ex for ex in File.load(filenames)]

        out_fns = {k: File.join(directory, f'{dataset_name}_{k}.jsonl') for k in list(split_dict.keys())}
        out_fns['results'] = File.join(directory, f'{dataset_name}_results.json')
        split_data = File.split_items(items, split_dict, shuffle)
        logger.info(f'Split Sizes for {len(filenames)} Files: {split_data["split_sizes"]}.\nOutput Files: {out_fns}')
        data = split_data.pop('data')

        res_meta = {'filenames': filenames, 'total_files': len(filenames), 'output_files': out_fns}
        res_meta.update(split_data)

        for split_key in split_dict:
            if output_format in ['jsonl', 'jsonlines', 'jl', 'jlines']:
                File.jlwrites(data[split_key], out_fns[split_key], mode='w')
            
            else:
                logger.error(f'Format {output_format} is not supported')
                assert ValueError
            
        logger.info(f'Final Metadata: {res_meta}')
        File.jsondump(res_meta, out_fns['results'])
        return res_meta


    def __call__(self, filename, mode='r', **kwargs):
        if kwargs.get('auto'):
            return self.open(filename, mode, **kwargs)
        return gfile.GFile(filename, mode)


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

    @property
    def index(self):
        return self.line_map
    
    def get_index(self):
        return self.line_map
    
    def __len__(self):
        return len(self.line_map)

    def __getitem__(self, index):
        # NOTE: This assumes that you're not reading the file sequentially.  
        # For that, just use 'for line in file'.
        self.fin.seek(self.line_map[index])
        return self.fin.readline()
