from tqdm.auto import tqdm
from fileio.core.generic_path import get_pathlike
from fileio.core.libs import JSON_FUNC, JSON_PARSER

# improved JSON parser

def read_jsonlines(filepath, ignore_errors: bool = True, parse_obj: bool = False, **kwargs):
    filepath = get_pathlike(filepath)
    with filepath.open(mode='rb+') as f:
        pbar = tqdm(f, desc=filepath.name)
        for _, line in enumerate(pbar):
            try:
                l = JSON_PARSER.parse(line, recursive=parse_obj, **kwargs)
                yield l
                del l

            except Exception as e:
                if ignore_errors:
                    continue
                raise e
            pbar.update()
        pbar.close()


def read_json(filepath, parse_obj: bool = False, **kwargs):
    filepath = get_pathlike(filepath)
    if not parse_obj:
        return JSON_FUNC.load(filepath.read_bytes(), **kwargs)
    return JSON_PARSER.parse(filepath.read_bytes(), **kwargs)


def autojson(filepath, parse_obj: bool = False, ignore_errors: bool = True, **kwargs):
    dtype = filepath.file_ext
    if dtype == 'json':
        return read_json(filepath, parse_obj=parse_obj, **kwargs)
    return read_jsonlines(filepath, ignore_errors=ignore_errors, parse_obj=parse_obj, **kwargs)



