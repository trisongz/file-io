

"""
Handler to check Class Imports
"""
import os
import sys
import json
import importlib
import subprocess
import pkg_resources
import pathlib

from typing import Any, Union, Dict
from types import ModuleType

def get_variable_separator():
    """
    Returns the environment variable separator for the current platform.
    :return: Environment variable separator
    """
    return ';' if sys.platform.startswith('win') else ':'

_lib_resolver_file = pathlib.Path(__file__).parent.joinpath('.lazylib.json')

class LibModuleType(type):
    
    @classmethod
    def get_requirement(cls, name: str, clean: bool = True) -> str:
        # Replaces '-' with '_'
        # for any library such as tensorflow-text -> tensorflow_text
        name = name.replace('-', '_')
        return name.split('=')[0].replace('>', '').replace('<', '').strip() if clean else name.strip()
    
    @classmethod
    def install_library(cls, library: str, upgrade: bool = True):
        pip_exec = [sys.executable, '-m', 'pip', 'install']
        if '=' not in library or upgrade: pip_exec.append('--upgrade')
        pip_exec.append(library)
        return subprocess.check_call(pip_exec, stdout=subprocess.DEVNULL)

    @classmethod
    def is_available(cls, library: str) -> bool:
        """ Checks whether a Python Library is available."""
        if library == 'colab':  library = 'google.colab'
        try:
            _ = pkg_resources.get_distribution(library)
            return True
        except pkg_resources.DistributionNotFound: return False
    
    @classmethod
    def is_imported(cls, library: str) -> bool:
        """ Checks whether a Python Library is currently imported."""
        return library in sys.modules
    
    @classmethod
    def _ensure_lib_imported(cls, library: str):
        clean_lib = cls.get_requirement(library, True)
        if not cls.is_imported(clean_lib): sys.modules[clean_lib] = importlib.import_module(clean_lib)
        return sys.modules[clean_lib]
    
    @classmethod
    def _ensure_lib_installed(cls, library: str, pip_name: str = None, upgrade: bool = False):
        clean_lib = cls.get_requirement(library, True)
        if not cls.is_available(clean_lib):
            cls.install_library(pip_name or library, upgrade=upgrade)

    @classmethod
    def import_lib(cls, library: str, pip_name: str = None, resolve_missing: bool = True, require: bool = False, upgrade: bool = False) -> ModuleType:
        """ Lazily resolves libs.

            if pip_name is provided, will install using pip_name, otherwise will use libraryname

            ie ->   Lib.import_lib('fuse', 'fusepy') # if fusepy is not expected to be available, and fusepy is the pip_name
                    Lib.import_lib('fuse') # if fusepy is expected to be available
            
            returns `fuse` as if you ran `import fuse`
        
            if available, returns the sys.modules[library]
            if missing and resolve_missing = True, will lazily install
        else:
            if require: raise ImportError
            returns None
        """
        clean_lib = cls.get_requirement(library, True)
        if not cls.is_available(clean_lib):
            if require and not resolve_missing: raise ImportError(f"Required Lib {library} is not available.")
            if not resolve_missing: return None
            cls.install_library(pip_name or library, upgrade=upgrade)
        return cls._ensure_lib_imported(library)
    
    @classmethod
    def import_module(cls, name: str, library: str = None, pip_name: str = None, resolve_missing: bool = True, require: bool = False, upgrade: bool = False) -> ModuleType:
        """ Lazily resolves libs and imports the name, aliasing
            immportlib.import_module

            ie ->   Lib.import_module('tensorflow.io.gfile', 'tensorflow') # if tensorflow is not expected to be available
                    Lib.import_module('tensorflow.io.gfile') # if tensorflow is expected to be available
            returns tensorflow.io.gfile
        """
        if library:
            cls.import_lib(library, pip_name, resolve_missing, require, upgrade)
            return importlib.import_module(name, package=library)
        return importlib.import_module(name)

    @classmethod
    def import_module_attr(cls, name: str, module_name: str, library: str = None, pip_name: str = None, resolve_missing: bool = True, require: bool = False, upgrade: bool = False) -> Any:
        """ Lazily resolves libs and imports the name, aliasing
            immportlib.import_module
            Returns an attribute from the module

            ie ->   Lib.import_module_attr('GFile', 'tensorflow.io.gfile', 'tensorflow') # if tensorflow is not expected to be available
                    Lib.import_module_attr('GFile', 'tensorflow.io.gfile') # if tensorflow is expected to be available
            returns GFile
        """
        mod = cls.import_module(name=module_name, library=library, pip_name=pip_name, resolve_missing = resolve_missing, require = require, upgrade = upgrade)
        return getattr(mod, name)
    

    @classmethod
    def get_binary_path(cls, executable: str):
        """
        Searches for a binary named `executable` in the current PATH. If an executable is found, its absolute path is returned
        else None.
        :param executable: Name of the binary
        :return: Absolute path or None
        """
        if 'PATH' not in os.environ: return None
        for directory in os.environ['PATH'].split(get_variable_separator()):
            binary = os.path.abspath(os.path.join(directory, executable))
            if os.path.isfile(binary) and os.access(binary, os.X_OK): return binary
        return None
    
    @classmethod
    def is_exec_available(cls, executable: str) -> bool:
        return cls.get_binary_path(executable) is not None
    
    @staticmethod
    def reload_module(module: ModuleType):
        return importlib.reload(module)
    
    @staticmethod
    def get_cwd(*paths, string: bool = True) -> Union[str, pathlib.Path]:
        if not paths:
            return pathlib.Path.cwd().as_posix() if string else pathlib.Path.cwd()
        if string: return pathlib.Path.cwd().joinpath(*paths).as_posix()
        return pathlib.Path.cwd().joinpath(*paths)
    
    @staticmethod
    def run_cmd(cmd, raise_error: bool = True):
        try:
            out = subprocess.check_output(cmd, shell=True)
            if isinstance(out, bytes): out = out.decode('utf8')
            return out.strip()
        except Exception as e:
            if not raise_error: return ""
            raise e

    def __getattr__(cls, key: str):
        """
            Lib.is_avail_tensorflow -> bool
            Lib.tensorflow -> sys.modules[tensorflow] or None
            
            Lib.is_avail_pydantic -> True (since its installed with this lib)
            Lib.is_imported_tensorflow 
            Lib.is_avail_bin_bash -> True
            Lib.is_avail_exec_bash -> True
        """
        if key.startswith('is_avail_bin_'):
            exec_name = key.split('is_avail_bin_')[-1].strip()
            return cls.is_exec_available(exec_name)
        
        if key.startswith('is_avail_exec_'):
            exec_name = key.split('is_avail_exec_')[-1].strip()
            return cls.is_exec_available(exec_name)
        
        if key.startswith('is_avail_lib_'):
            lib_name = key.split('is_avail_lib_')[-1].strip()
            return cls.is_available(lib_name)
        
        if key.startswith('is_avail_'):
            lib_name = key.split('is_avail_')[-1].strip()
            return cls.is_available(lib_name)
        
        if key.startswith('is_imported_'):
            lib_name = key.split('is_imported_')[-1].strip()
            return cls.is_imported(lib_name)
        
        return cls.import_lib(key, resolve_missing=False, require=False)
    
    @classmethod
    def get(cls, name: str, attr_name: str = None, pip_name: str = None, resolve_missing: bool = True) -> Union[ModuleType, Any]:
        """
        Resolves the import based on params.

        Importing a module:
        Lib.get('tensorflow') -> tensorflow Module
        Lib.get('fuse', pip_name='fusepy') -> fuse Module
        
        Importing a submodule:
        Lib.get('tensorflow.io') -> io submodule

        Importing something from within a submodule:
        Lib.get('tensorflow.io.gfile', 'GFile') -> GFile class
        """
        if attr_name: 
            lib_name = name.split('.', 1)[0]
            return cls.import_module_attr(attr_name, module_name = name, library = lib_name, pip_name = pip_name, resolve_missing = resolve_missing)
        if '.' in name: 
            lib_name = name.split('.', 1)[0]
            return cls.import_module(name, library=lib_name, pip_name=pip_name, resolve_missing=resolve_missing)
        return cls.import_lib(name, pip_name=pip_name, resolve_missing=resolve_missing)
    
    @classmethod
    def _parse_name(cls, name: str) -> Dict[str, str]:
        """
        Resolves the name into a dict = 
        {
            'library': str,
            'pip_name': Optional[str],
            'module_name': Optional[str],
            'attr_name': Optional[str]
        }
        
        - module.submodule:attribute
        - pip_name|module.submodule:attribute # if pip_name is not the same
        
        Lib['tensorflow']                   -> {'library': 'tensorflow'}
        Lib['fusepy|fuse']                  -> {'library': 'fuse', 'pip_name': 'fusepy'}
        Lib['tensorflow.io']                -> {'library': 'tensorflow', 'module_name': 'tensorflow.io'}
        Lib['tensorflow.io.gfile:GFile']    -> {'library': 'tensorflow', 'module_name': 'tensorflow.io.gfile', 'attr_name': GFile}
        """
        rez = {'library': '', 'pip_name': None, 'module_name': None, 'attr_name': None}
        _name = name.strip()
        if ':' in _name:
            _s = _name.split(':', 1)
            rez['attr_name'] = _s[-1]
            _name = _s[0]
        if '|' in _name:
            _s = _name.split('|', 1)
            rez['pip_name'] = _s[0]
            _name = _s[-1]
        if '.' in _name:
            _s = _name.split('.', 1)
            rez['module_name'] = _name
            rez['library'] = _s[0]
        else: 
            #rez['module_name'] = _name
            rez['library'] = _name
        return rez
    
    @classmethod
    def __getitem__(cls, name: str) -> ModuleType:
        """
        Resolves the import based on params. 
        Will automatically assume as resolve_missing = True, require = True
        
        The naming scheme is resolved as
        
        - module.submodule:attribute
        - pip_name|module.submodule:attribute # if pip_name is not the same

        Importing a module:
        Lib['tensorflow'] -> tensorflow Module
        Lib['fusepy|fuse'] -> fuse Module
        
        Importing a submodule:
        Lib['tensorflow.io'] -> io submodule

        Importing something from within a submodule:
        Lib['tensorflow.io.gfile:GFile'] -> GFile class
        """
        r = cls._parse_name(name)
        if r.get('attr_name'): return cls.import_module_attr(r['attr_name'], module_name = r['module_name'], library = r['library'], pip_name = r['pip_name'], resolve_missing = True, require = True)
        if r.get('module_name'): return cls.import_module(name = r['module_name'], library = r['library'], pip_name = r['pip_name'], resolve_missing = True, require = True)
        return cls.import_lib(r['library'], pip_name = r['pip_name'], resolve_missing = True, require = True)

        
class LazyLib(metaclass=LibModuleType):
    pass