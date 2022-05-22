
import os
import sys
import importlib
import threading
import subprocess
import pkg_resources
import json
from subprocess import check_output
from abc import abstractmethod

from fileio.core import logger
from fileio.configs import ROOT_DIR, ENABLE_PROGRESS_BAR

_importlock = threading.Lock()
_imports = {}

def lazy_import(name):
    if name in sys.modules:
        return sys.modules[name]
    sys.modules[name] = importlib.import_module(name)
    return sys.modules[name]

def exec_command(cmd):
    out = check_output(cmd, shell=True)
    if isinstance(out, bytes):
        out = out.decode('utf8')
    return out.strip()

def gsutil_exec(command, pbar = None, verbose = True):
    """
    Runs a given command using subprocess module
    """
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, bufsize=1)
    for line in iter(p.stdout.readline, ''):
        if not line:
            break
        if isinstance(line, bytes): line = line.decode('utf8')
        line = line.replace('\r', '').replace('\n', '')
        if line and verbose:
            if pbar:
                pbar.write(line)
            else:
                print(line)
                sys.stdout.flush()

def lazy_check(req):
    try:
        _ = pkg_resources.get_distribution(req)
        return True
    except pkg_resources.DistributionNotFound:
        return False

def lazy_install(req):
    _req = req.split('=')[0].replace('>','').replace('<','').strip()
    if lazy_check(_req):
        return
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', req], stdout=subprocess.DEVNULL)

def _setup_lib(name, required):
    global _imports
    with _importlock:
        if name in _imports:
            return
        if required:
            lazy_install(name)
        elif not lazy_check(name):
            raise ValueError
        _imports[name] = lazy_import(name)

def initialize_lib(name, required=False):
    _setup_lib(name)
    return _imports[name]

class LazyLib:
    def __init__(self, name, required=False):
        self._name = name
        self._lib = None
        self._required = required
    
    def _setup(self):
        self._lib = initialize_lib(self._name, self._required)
    
    def __call__(self, *args, **kwargs):
        if not self._lib: self._setup()
        return self._lib


class Parser:
    def __init__(self):
        pass

    @abstractmethod
    def _filename(self, index, basename=False, absolute=False):
        pass

    def filename(self, index, basename=False, absolute=False):
        return self._filename(index, basename=basename, absolute=absolute)

    def filenames(self, basename=False, absolute=False):
        return [self._filename(index, basename=basename, absolute=absolute) for index in range(len(self))]
