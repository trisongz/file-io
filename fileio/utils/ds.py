import copy
from dataclasses import dataclass, asdict, field
from typing import Any, List, Optional, Union, Callable, Dict


from . import logger
from ..src import File

_datacachedir = ''

def default_field(obj):
    return field(default_factory=lambda: copy.copy(obj))

@dataclass
class TFDSIODataset:
    name: str
    dataset_urls: Dict[str, str] = default_field({"train": ""})
    classifier: Optional[str] = 'lm'
    version: Optional[str] = '1.0.0'
    features: Optional[Dict[str, str]] = default_field({'inputs': 'text', 'targets': 'text'})
    datamap: Optional[Dict[str, str]] = default_field({'inputs': 'inputs', 'targets': 'targets'})
    dataset_format: Optional[str] = 'jsonlines'
    homepage: Optional[str] = 'https://growthengineai.com'
    description: Optional[str] = ''
    data_dir: Optional[str] = _datacachedir

    def set_datadir(self, data_dir, overwrite=False):
        global _datacachedir
        if _datacachedir == data_dir:
            return
        if _datacachedir and not overwrite:
            logger.info(f'Data Dir is currently = {_datacachedir}. Attempting to set to {data_dir} but overwrite = False')
            return
        _datacachedir = data_dir
        logger.info(f'Setting Data Dir = {data_dir}')
    
    @property
    def config(self):
        self.data_dir = _datacachedir
        d = asdict(self)
        for k in ['dataset_path', 'config_path', 'config']:
            if d.get(k):
                _ = d.pop(k)
        return d

    @property
    def dataset_path(self):
        self.data_dir = _datacachedir
        return File.join(self.data_dir, self.name, self.classifier, self.version)
    
    @property
    def config_path(self):
        return File.join(self.dataset_path, 'tfdsio_config.json')

    def save_config(self):
        logger.info(f'Saving Config to {self.config_path}')
        File.jsondump(self.config, self.config_path)
    
    def load_config(self):
        if not File.exists(self.config_path):
            logger.info(f'Config does not exist {self.config_path}')
            return
        logger.info(f'Loading from {self.config_path}')
        conf = File.jsonload(self.config_path)
        for k,v in conf.items():
            self.__dict__[k] = v

    def sync_bucket(self, data_dir, overwrite=False):
        new_path = File.join(data_dir, self.name, self.classifier, self.version)
        if File.pexists(new_path, 'dataset_info.json') and not overwrite:
            logger.info(f'Dataset Exists at {new_path} and Overwrite = False')
            return
        File.mkdirs(new_path)
        logger.info(f'Syncing from {self.dataset_path} -> {new_path}')
        File.gsutil(f'cp -r {self.dataset_path}/* {new_path}/', multi=True)
        new_files = File.glob(new_path + '/*')
        logger.info(f'Copied {len(new_files)} to {new_path}')
        return new_files







