
from pathlib import Path
from typing import List, Dict, Union, Any

try:
    import ruamel.yaml
    _yaml_available = True
except ImportError:
    _yaml_available = False


if _yaml_available:
    yaml = ruamel.yaml.YAML(typ=['rt', 'string'])
    yaml.preserve_quotes = True
    yaml.indent(
        mapping = 2, 
        sequence = 2, 
        offset = 0
    )


    class Yaml:

        @classmethod
        def dump(cls, obj: Dict[Any, Any], stream: Any = None, indent = 2, *args, **kwargs) -> Any:
            return yaml.dump(obj, stream = stream, indent = indent, *args, **kwargs)

        @classmethod
        def dumps(cls, obj: Dict[Any, Any], stream: Any = None, *args, **kwargs) -> Any:
            return yaml.dump_to_string(obj, *args, **kwargs)

        @classmethod
        def load(cls, data: Union[str, bytes, Any], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
            return yaml.load(data, *args, **kwargs)
        
        @classmethod
        def loads_multi(cls, data: Union[str, bytes, Any], *args, _seperator: str = '\n---', **kwargs) -> Union[Dict[Any, Any], List[Dict[Any, Any]]]:
            """
            Loads a multi-yaml string into a list of Dict[Yaml].
            """
            _data = data.split(_seperator)
            try: return [yaml.load(d, *args, **kwargs) for d in _data]
            except Exception as e: 
                return yaml.load(data, *args, **kwargs)

        @classmethod
        def loads(cls, data: Union[str, bytes, Path, Any], *args, multi: bool = False, **kwargs) -> Union[Dict[Any, Any], List[str], List[Dict[Any, Any]]]:
            if not isinstance(data, str) and hasattr(data, 'as_posix'):
                # If it's a file-like object, read it as a string
                data = data.read_text()
            if '\n---' in data: multi = True 
            if multi: return cls.loads_multi(data, *args, **kwargs)
            return yaml.load(data, *args, **kwargs)



else:

    class Yaml:
        @classmethod
        def dump(cls, obj: Dict[Any, Any], stream: Any = None, indent = 2, *args, **kwargs) -> Any:
            raise ImportError("ruamel.yaml is not installed.")

        @classmethod
        def dumps(cls, obj: Dict[Any, Any], stream: Any = None, *args, **kwargs) -> Any:
            raise ImportError("ruamel.yaml is not installed.")

        @classmethod
        def load(cls, data: Union[str, bytes, Any], *args, **kwargs) -> Union[Dict[Any, Any], List[str]]:
            raise ImportError("ruamel.yaml is not installed.")
        
        @classmethod
        def loads_multi(cls, data: Union[str, bytes, Any], *args, _seperator: str = '\n---', **kwargs) -> Union[Dict[Any, Any], List[Dict[Any, Any]]]:
            """
            Loads a multi-yaml string into a list of Dict[Yaml].
            """
            raise ImportError("ruamel.yaml is not installed.")
            

        @classmethod
        def loads(cls, data: Union[str, bytes, Path, Any], *args, multi: bool = False, **kwargs) -> Union[Dict[Any, Any], List[str], List[Dict[Any, Any]]]:
            raise ImportError("ruamel.yaml is not installed.")
            
