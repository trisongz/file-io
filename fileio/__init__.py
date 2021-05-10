import os
import simdjson as json
import pickle
import csv
import warnings
import requests
import gdown as gdownload
import tempfile
from tqdm.auto import tqdm
from fileio.utils import lazy_import, logger, Auth

try:
    import torch
    _torch_avail = True
except ImportError:
    _torch_avail = False

warnings.filterwarnings("ignore", category=DeprecationWarning)
gf = lazy_import('tensorflow.io.gfile')
gfile = gf.GFile
glob = gf.glob
gcopy = gf.copy
isdir = gf.isdir
listdir = gf.listdir
mkdirs = gf.makedirs
mv = gf.rename
exists = gf.exists
rmdir = gf.rmtree
rm = gf.remove
jparser = json.Parser()
cd = os.chdir
curdir = os.getcwd

tf = lazy_import('tensorflow')
TextLineDataset = tf.data.TextLineDataset
TFRecordDataset = tf.data.TFRecordDataset
TFRecordWriter = tf.io.TFRecordWriter
AUTOTUNE = tf.data.experimental.AUTOTUNE
enable_eager_execution = tf.compat.v1.enable_eager_execution
disable_v2_behavior = tf.compat.v1.disable_v2_behavior

_tmpdirs = []

class File(object):
    @classmethod
    def join(cls, path, *paths):
        return os.path.join(path, *paths)

    @classmethod
    def listfiles(cls, filepath):
        return listdir(filepath)

    @classmethod
    def isfile(cls, filepath):
        return isfile(filepath)

    @classmethod
    def listdir(cls, filepath):
        return listdir(filepath)

    @classmethod
    def mkdir(cls, filepath):
        return mkdirs(filepath)

    @classmethod
    def makedirs(cls, filepath):
        return mkdirs(filepath)

    @classmethod
    def mkdirs(cls, filepath):
        return mkdirs(filepath)

    @classmethod
    def glob(cls, filepath):
        return glob(filepath)

    @classmethod
    def mv(cls, src, dest, overwrite=False):
        return mv(src, dest, overwrite)

    @classmethod
    def rm(cls, filename):
        return rm(filename)
    
    @classmethod
    def rmdir(cls, filepath):
        return rmdir(filepath)

    @classmethod
    def copy(cls, src, dest, overwrite=True):
        return gcopy(src, dest, overwrite)
    
    @classmethod
    def bcopy(cls, src, directory, overwrite=True):
        if not isdir(directory):
            mkdirs(directory)
        dest = os.path.join(directory, os.path.basename(src))
        gcopy(src, dest, overwrite)
        return dest

    @classmethod
    def exists(cls, filepath):
        return exists(filepath)
    
    @classmethod
    def pexists(cls, path, *paths):
        return exists(File.join(path, *paths))
    
    @classmethod
    def whichpath(cls, path1, path2):
        if exists(path1):
            return path1
        if exists(path2):
            return path2
        raise ValueError

    @classmethod
    def base(cls, filepath):
        return os.path.basename(filepath)

    @classmethod
    def ext(cls, filepath):
        f = os.path.basename(filepath)
        _, e = os.path.splitext(f)
        return e

    @classmethod
    def writemode(cls, filepath, overwrite=False):
        if exists(filepath):
            return 'a'
        return 'w'
    
    @classmethod
    def touch(cls, filepath, overwrite=False):
        if not exists or overwrite:
            with gfile(filepath, 'w') as f:
                f.write('\n')
                f.flush()
            f.close()

    @classmethod
    def open(cls, filename, mode='r', auto=True, device=None):
        if 'r' in mode and auto:
            if filename.endswith('.pkl'):
                return File.pload(filename)
            
            if filename.endswith('.jsonl') or filename.endswith('.jsonlines'):
                return File.jg(filename)
            
            if filename.endswith('.json'):
                return File.jsonload(filename)
            
            if filename.endswith('.pt'):
                return File.ptload(filename, device)

        return gfile(filename, mode)
    
    @classmethod
    def save(cls, data, filename, overwrite=False):
        if filename.endswith('.pkl'):
            return File.pklsave(data, filename)
        
        if filename.endswith('.jsonl') or filename.endswith('.jsonlines'):
            return File.jlw(data, filename)
        
        if filename.endswith('.json'):
            return File.jsondump(data, filename)
        
        if filename.endswith('.pt'):
            return File.ptsave(data, filename)
        
        if filename.endswith('.txt'):
            return File.txtwrite(data, filename, overwrite)
        
        logger.info('Unrecognized Extension. Not Saving')
        return
    
    @classmethod
    def load(cls, filenames, device=None):
        filenames = File.fsorter(filenames)
        _is_tfr = bool(sum(1 for fn in filenames if fn.endswith('.tfrecords')) > 1)
        if _is_tfr:
            return File.tfreader(filenames)
        for filename in filenames:
            if filename.endswith('.pkl'):
                yield File.pload(filename)
            
            elif filename.endswith('.jsonl') or filename.endswith('.jsonlines'):
                yield File.jg(filename)
            
            elif filename.endswith('.json'):
                yield File.jsonload(filename)
            
            elif filename.endswith('.pt'):
                yield File.ptload(filename, device)
            
            elif filename.endswith('.txt'):
                yield File.txtload(filename)
            
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

    
    @classmethod
    def writer(cls, filename, mode='w', auto=True, overwrite=False):
        if filename.endswith('.tfrecords'):
            return File.tfwriter(filename)
        if auto:
            mode = File.writemode(filename, overwrite)
        return gfile(filename, mode)


    @classmethod
    def write(cls, filename, mode='w'):
        return gfile(filename, mode)
    
    @classmethod
    def append(cls, filename, mode='a'):
        return gfile(filename, mode)
    
    @classmethod
    def read(cls, filename, mode='r'):
        return gfile(filename, mode)

    @classmethod
    def rb(cls, filename):
        return gfile(filename, 'rb')
    
    @classmethod
    def wb(cls, filename):
        return gfile(filename, 'wb')

    @classmethod
    def readlines(cls, filename):
        with gfile(filename, 'r') as f:
            return f.readlines()

    @classmethod
    def pklsave(cls, obj, filename):
        return pickle.dump(obj, gfile(filename, 'wb'))

    @classmethod
    def pload(cls, filename):
        return pickle.load(gfile(filename, 'rb'))
    
    @classmethod
    def torchsave(cls, obj, filename):
        assert _torch_avail, 'pytorch is not available'
        return torch.save(obj, gfile(filename, 'wb'))

    @classmethod
    def torchload(cls, filename, device=None):
        assert _torch_avail, 'pytorch is not available'
        return torch.load(gfile(filename, 'rb'), map_location=device)
    
    @classmethod
    def ptsave(cls, obj, filename):
        return File.torchsave(obj, filename)
    
    @classmethod
    def ptload(cls, filename, device=None):
        return File.torchload(filename, device)
    
    @classmethod
    def pklload(cls, filename):
        return pickle.load(gfile(filename, 'rb'))

    @classmethod
    def csvload(cls, filename):
        return list(csv.reader(gfile(filename, 'r')))

    @classmethod
    def tsvload(cls, filename):
        return list(csv.reader(gfile(filename, 'r'), delimiter='\t'))
    
    @classmethod
    def csvdictload(cls, filename):
        return dict(csv.DictReader(gfile(filename, 'r')))

    @classmethod
    def tsvdictload(cls, filename):
        return dict(csv.DictReader(gfile(filename, 'r'), delimiter='\t'))


    @classmethod
    def txtload(cls, filename):
        with gfile(filename, 'r') as f:
            for line in f:
                yield line.strip()

    @classmethod
    def txtwrite(cls, data, filename, overwrite=False):
        mode = 'w' if overwrite or not exists(filename) else 'a'
        with gfile(filename, mode) as f:
            if isinstance(data, list):
                for d in data:
                    f.write(d + '\n')
            else:
                f.write(data + '\n')
            f.flush()


    @classmethod
    def jsonload(cls, filename):
        return json.load(gfile(filename, 'r'))
    
    @classmethod
    def jsonloads(cls, string):
        return json.loads(string)
    
    @classmethod
    def jsondump(cls, obj, filename, indent=2, ensure_ascii=False):
        return json.dump(obj, gfile(filename, 'w'), indent=indent, ensure_ascii=ensure_ascii)
    
    @classmethod
    def jsondumps(cls, pdict, ensure_ascii=False):
        return json.dumps(pdict, ensure_ascii=ensure_ascii)
    
    @classmethod
    def jp(cls, line):
        return jparser.parse(line).as_dict()

    @classmethod
    def jl(cls, line):
        return json.loads(line)
    
    @classmethod
    def jlp(cls, line):
        try:
            return File.jp(line)
        except:
            return File.jl(line)

    @classmethod
    def jldump(cls, data, f):
        f.write(json.dumps(data, ensure_ascii=False))
        f.write('\n')
    
    @classmethod
    def twrite(cls, data, f):
        f.write(data + '\n')

    @classmethod
    def flush(cls, f):
        f.flush()

    @classmethod
    def fclose(cls, f):
        f.flush()
        f.close()

    @classmethod
    def jg(cls, filename, handle_errors=True):
        with gfile(filename, 'r') as f:
            for l in f:
                try:
                    yield json.loads(l)
                except Exception as e:
                    if not handle_errors:
                        logger.log(f'Error parsing File: {str(e)}')
                        raise e
    
    @classmethod
    def autowrite(cls, filename, overwrite=False):
        if overwrite or not exists(filename):
            return 'w'
        return 'a'

    @classmethod
    def jlwrite(cls, data, filename, mode='auto'):
        mode = mode if mode != 'auto' else File.autowrite(filename)
        with gfile(filename, mode=mode) as f:
            File.jldump(data, f)
        File.fclose(f)

    @classmethod
    def jlwrites(cls, data_items, filename, mode='auto'):
        mode = mode if mode != 'auto' else File.autowrite(filename)
        with gfile(filename, mode=mode) as f:
            for data in data_items:
                File.jldump(data, f)
        File.fclose(f)

    @classmethod
    def jlload(cls, filename, as_iter=False, index=False, handle_errors=True):
        if as_iter:
            if not index:
                return File.jg(filename, handle_errors=handle_errors)
            yield from enumerate(File.jg(filename, handle_errors=handle_errors))

        else:
            if index:
                return {x: item for x, item in enumerate(File.jg(filename, handle_errors=handle_errors))}
            return [item for item in File.jg(filename, handle_errors=handle_errors)]

    @classmethod
    def jlw(cls, data, filename, mode='auto', verbose=True):
        mode = mode if mode != 'auto' else File.autowrite(filename)
        if isinstance(data, dict):
            return File.jlwrite(data, filename, mode=mode)
        _good, _bad, failed = 0, 0, []
        with gfile(filename, mode) as f:
            for d in data:
                try:
                    File.jldump(d, f)
                    _good += 1
                except Exception as e:
                    if verbose:
                        logger.info(f'Error: {str(e)} Writing Line {d}')
                    failed.append({'data': d, 'error': str(e)})
                    _bad += 1
            File.fclose(f)
        _total = _good + _bad
        logger.info(f'Wrote {_good}/{_total} Lines [Mode: {mode}] - Failed: {_bad}')
        return failed

    @classmethod
    def fsorter(cls, filenames):
        fnames = []
        if isinstance(filenames, str) or not isinstance(filenames, list):
            filenames = [filenames]
        for fn in filenames:
            if not isinstance(fn, str):
                fn = str(fn)
            if fn.endswith('*'):
                _newfns = glob(fn)
                _newfns = [f for f in _newfns if isfile(f) and exists(f)]
                fnames.extend(_newfns)
            elif not isdir(fn) and exists(fn):
                fnames.append(fn)
        return fnames

    @classmethod
    def jgs(cls, filenames, handle_errors=True):
        filenames = File.fsorter(filenames)
        for fname in filenames:
            yield File.jg(fname, handle_errors)

    def __call__(self, filename, mode='r'):
        return self.open(filename, mode)

    @classmethod
    def gfile(cls, filename, mode):
        return gfile(filename, mode)
    
    @classmethod
    def gfiles(cls, filenames, mode='r'):
        fnames = File.fsorter(filenames)
        for fn in fnames:
            yield gfile(fn, mode)
    
    @classmethod
    def tfeager(cls, enable=True):
        if enable:
            enable_eager_execution()
        else:
            disable_v2_behavior()
    
    @classmethod
    def tflines(cls, filenames):
        File.tfeager()
        fnames = File.fsorter(filenames)
        return TextLineDataset(fnames, num_parallel_reads=AUTOTUNE)
    
    @classmethod
    def csvreader(cls, f):
        return csv.DictReader(f)
    
    @classmethod
    def tsvreader(cls, f):
        return csv.DictReader(f, delimiter='\t')

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
                except Exception as e:
                    if verbose:
                        logger.info(f'Error on {idx}: {str(e)} - {x}')
    
    @classmethod
    def tfreader(cls, filenames, compression=None, buffer=None, num_parallel=AUTOTUNE):
        return TFRecordDataset(filenames, compression_type=compression, buffer_size=buffer, num_parallel_reads=num_parallel)
    
    @classmethod
    def tfwriter(cls, filename):
        return TFRecordWriter(filename)
    
    @classmethod
    def num_lines(cls, filenames):
        pipeline = File.tflines(filenames)
        return sum(1 for _ in tqdm(pipeline, desc='Getting Total Lines..'))
    
    @classmethod
    def download(cls, url, dirpath=None, filename=None, overwrite=False):
        if not filename:
            filename = File.base(url)
        if dirpath:
            File.join(dirpath, filename)
        if File.exists(filename) and not overwrite:
            logger.info(f'{filename} exists and overwrite = False')
            return
        rstream = requests.get(url, stream=True)
        with File.wb(filename) as f:
            for chunk in tqdm(rstream.iter_content(chunk_size=1024), desc=f'Downloading {filename}'):
                if not chunk:
                    break
                f.write(chunk)
            f.flush()
        f.close()
    
    @classmethod
    def absdownload(cls, url, filepath, overwrite=False):
        if File.exists(filepath) and not overwrite:
            logger.info(f'{filepath} exists and overwrite = False')
            return
        rstream = requests.get(url, stream=True)
        with File.wb(filepath) as f:
            for chunk in tqdm(rstream.iter_content(chunk_size=1024), desc=f'Downloading {filepath}'):
                if not chunk:
                    break
                f.write(chunk)
            f.flush()
        f.close()

    @classmethod
    def batch_download(cls, urls, directory=None, overwrite=False):
        if not directory:
            directory = curdir()
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
        url = File.gurl(url)
        if extract:
            return gdownload.cached_download(url, postprocess=gdownload.extractall, quiet=verbose)
        return gdownload.download(url, quiet=verbose)

    @classmethod
    def batch_gdown(cls, urls, directory=None, extract=True, verbose=False):
        if directory:
            cd(directory)
            logger.info(f'Downloading into: {directory}')
        else:
            logger.info(f'No directory set. Using: {curdir()}')
        logger.info(f'Downloading {len(urls)} Urls')
        for url in urls:
            try:
                File.gdown(url, extract, verbose)
            except Exception as e:
                logger.info(f'Failed to download {url}: {str(e)}')

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
            #rmdir(tmpdir)





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