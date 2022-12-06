
from typing import Dict, Any, Union, List
from fileio.io._base import BasePack

class Text:
    list_delimiters: List[str] = [",", ";"]
    dict_delimiters: List[str] = ["=", "|", ":"]
    true_values: List[str] =  ['true', 'yes']
    false_values: List[str] = ['false', 'no']
    none_values: List[str] = ['', 'none', 'null', 'n/a']

    @classmethod
    def cast_value(cls, text: str):
        text = text.lower()
        for v in cls.true_values:
            if text == v: return True
        for v in cls.false_values:
            if text == v: return False
        for v in cls.none_values:
            if text == v: return None
        if '.' in text and text.isnumeric(): return float(text)
        return int(text) if text.isnumeric() else text
        

    @classmethod
    def to_list(cls, text: str) -> List[str]:
        return next(([i for i in text.split(delim) if i] for delim in cls.list_delimiters if delim in text), [text])

    @classmethod
    def to_dict(cls, text: str) -> Dict[str, Any]:
        items = cls.to_list(text)
        if not items: return {}
        rez = {}
        for i in items:
            for d in cls.dict_delimiters:
                if d in i:
                    for k,v in i.split(d, 1):
                        rez[k.strip()] = cls.cast_value(v.strip())
        return rez