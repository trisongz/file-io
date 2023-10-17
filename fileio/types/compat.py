"""
Resolver for Pydantic v1/v2 imports with additional helpers
"""
import os
import sys 
import typing

try:
    from pydantic import validator as _validator
    from pydantic import model_validator as base_root_validator

    PYD_VERSION = 2

    def root_validator(*args, **kwargs):
        """
        v1 Compatible root validator
        """
        def decorator(func):
            _pre_kw = kwargs.pop('pre', None)
            if _pre_kw is True:
                kwargs['mode'] = 'before'
            return base_root_validator(*args, **kwargs)(func)
        return decorator

    def pre_root_validator(*args, **kwargs):
        def decorator(func):
            return base_root_validator(*args, mode='before', **kwargs)(func)
        return decorator
    
    def validator(*args, **kwargs):
        def decorator(func):
            return _validator(*args, **kwargs)(classmethod(func))
        return decorator

except ImportError:
    from pydantic import root_validator, validator

    PYD_VERSION = 1

    def pre_root_validator(*args, **kwargs):
        def decorator(func):
            return root_validator(*args, pre=True, **kwargs)(func)
        return decorator



try:
    from pydantic_settings import BaseSettings

except ImportError:
    if PYD_VERSION == 2:
        os.system(f'{sys.executable} -m pip install pydantic-settings')
        from pydantic_settings import BaseSettings
    else:
        from pydantic import BaseSettings

from pydantic import BaseModel, Field

def get_pyd_dict(model: typing.Union[BaseModel, BaseSettings], **kwargs) -> typing.Dict[str, typing.Any]:
    """
    Get a dict from a pydantic model
    """
    if kwargs: kwargs = {k:v for k,v in kwargs.items() if v is not None}
    return model.model_dump(**kwargs) if PYD_VERSION == 2 else model.dict(**kwargs)