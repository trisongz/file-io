
import os
import sys
import importlib
import threading
import subprocess
import pkg_resources
import json
from subprocess import check_output
from abc import abstractmethod
from . import logger

root = os.path.abspath(os.path.dirname(__file__))


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
        else:
            if not lazy_check(name):
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
        if not self._lib:
            self._setup()
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

auth_vars = {
    'adc': 'GOOGLE_APPLICATION_CREDENTIALS',
    'gproject': 'GOOGLE_CLOUD_PROJECT'
}
auth_file = os.path.join(root, 'utils/config.json')

class Auth(object):
    def __init__(self, adc=None, project=None, overwrite=False):
        data = self.load()
        if adc:
            data['adc'] = adc
            self.set_adc(adc)
        elif data.get('adc', None) and not overwrite:
            self.set_adc(data['adc'])
        else:
            adc_var = self.check(auth_vars['adc'])
            if adc_var:
                data['adc'] = adc_var
            else:
                logger.info(f'ADC is not found or set')

        if project:
            data['project'] = project
            self.set_gproject(project)
        elif data.get('project', None) and not overwrite:
            self.set_gproject(data['project'])
        else:
            gp_var = self.check(auth_vars['gproject'])
            if gp_var:
                data['project'] = gp_var
        self.save(data)

    
    @classmethod
    def set_adc(cls, adc):
        logger.info(f'Setting ADC to {adc}')
        os.environ[auth_vars['adc']] = adc
    
    @classmethod
    def set_gproject(cls, project):
        logger.info(f'Setting GCP PRoject to {project}')
        os.environ[auth_vars['gproject']] = project
    
    @classmethod
    def check(cls, name):
        return os.environ.get(name, None)
    
    @classmethod
    def load(cls):
        if not os.path.exists(auth_file):
            return {}
        return json.load(open(auth_file, 'r'))

    @classmethod
    def save(cls, data):
        json.dump(open(auth_file, 'w'), indent=2)
