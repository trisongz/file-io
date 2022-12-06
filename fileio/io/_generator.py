import string
import secrets
import uuid as _uuid
from typing import Dict, Any
from fileio.io._base64 import Base64

ALPHA_NUMERIC = string.ascii_letters + string.digits

class Generate:
    default_method: str = 'uuid4'

    @classmethod
    def uuid(cls, method: str = None, *args, **kwargs):
        method = method or cls.default_method
        t = getattr(_uuid, method, cls.default_method)
        return str(t(*args, **kwargs))

    @classmethod
    def uuid_passcode(cls, length: int = None, clean: bool = True, method: str = None):
        rez = cls.uuid(method=method)
        if clean: rez = rez.replace('-', '').strip()
        if length: rez = rez[:length]
        return rez
    
    @classmethod
    def alphanumeric_passcode(cls, length: int = 16):
        return ''.join(secrets.choice(ALPHA_NUMERIC) for _ in range(length))
    
    @classmethod
    def token(cls, length: int = 32, safe: bool = False, clean: bool = True):
        rez = secrets.token_hex(length) if safe else secrets.token_urlsafe(length)
        if clean:
            for i in rez: 
                if i not in ALPHA_NUMERIC: rez.replace(i, secrets.choice(ALPHA_NUMERIC))
        return rez
    
    @classmethod
    def openssl_random_key(cls, length: int = 64, base: bool = True):
        # openssl rand 64 | base64
        key = secrets.token_hex(length)
        if base: key = Base64.encode(key)
        return key
    
    @classmethod
    def keypair(cls, key_length: int = 16, secret_length: int = 36) -> Dict[str, str]:
        return {
            'key': cls.alphanumeric_passcode(key_length),
            'secret': cls.alphanumeric_passcode(secret_length) 
        }


        



