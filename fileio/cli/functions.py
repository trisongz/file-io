from __future__ import annotations

"""
FileIO CLI Handler
"""

import typer
import base64
from pydantic.types import ByteSize
from fileio.lib.types import File
from lazyops.utils.helpers import timer
from typing import Optional, List, Union, Dict, Any, Callable, TYPE_CHECKING

async def fetch_file(
    source: str,
    destination: str,
    filename: Optional[str] = None,
    new_suffix: Optional[str] = None,
    overwrite: Optional[bool] = False,
) -> File:
    """
    Uploads a file using FileIO backend

    Source: Path to file to upload
    Destination: Path to upload file to. Can be a directory or a file
    Filename: Name of file to upload as. If not provided, will use the name of the source file
    NewSuffix: If provided, will replace the suffix of the source file with this suffix
    Overwrite: Whether to overwrite the destination file if it exists
    """

    source_path = File(source)
    destination_path = File(destination)

    assert await source_path.async_exists(), f'File {source_path} does not exist'
    assert await source_path.async_is_file(), f'File {source_path} is not a file'
    if await destination_path.async_exists() and not overwrite:
        raise FileExistsError(f'File {destination_path} already exists')
    
    t = timer()
    if destination_path.is_dir():
        await destination_path.async_mkdir(parents = True, exist_ok = True)
        filename = filename or source_path.name
        destination_path = destination_path.joinpath(filename)
        if new_suffix:
            destination_path = destination_path.with_suffix(new_suffix)
    
    await source_path.async_copy(destination_path, overwrite = overwrite)
    typer.echo(f'Copied {source_path} to {destination_path} in {timer(t):.2f} secs')
    return destination_path


async def source_info_file(
    source: str,
) -> Dict[str, Any]:
    """
    Returns information about a file or directory using FileIO backend
    """
    source_path = File(source)
    if not await source_path.async_exists():
        raise FileNotFoundError(f'File {source_path} does not exist')
    
    t = timer()
    stat = await source_path.async_info()
    # typer.echo(f'Got stats for {source_path} in {timer(t):.2f} secs')
    for key, value in stat.items():
        typer.echo(f'{key}: {value}')
    
    typer.echo(f'Retrieved stats for {source_path} in {timer(t):.2f} secs')
    return stat


async def source_info(
    source: str,
    recursive: Optional[bool] = False,
    dirs_only: Optional[bool] = False,
) -> Union[List[File], List[Dict[str, Any]]]:
    """
    Returns information about a file or directory using FileIO backend
    """

    source_path = File(source)
    if not await source_path.async_exists():
        raise FileNotFoundError(f'File {source_path} does not exist')
    if not await source_path.async_is_dir():
        return await source_info_file(source)

        # typer.echo(f'File {source_path} is not a directory. Listing parent directory instead')
        # source_path = source_path.parent

    t = timer()
    stats = await source_path.async_ls(
        recursive = recursive, 
        detail = True,
        prettify = True,
        as_path = False,
        files_only = not dirs_only,
    )
    # typer.echo(f'Retrieved stats for {source_path} in {timer(t):.2f} secs')

    total_size = sum(stat['Size'] for stat in stats)
    total_size_pretty = ByteSize.validate(total_size).human_readable()
    typer.echo(f'Source: {source_path}')
    for stat in stats:
        # typer.echo(f'Key: {stat["Key"]}')
        typer.echo(f'{stat["Key"]}')
        if stat.get('SizePretty'):
            typer.echo(f'- Size: {stat["SizePretty"]} ({stat["Size"]} bytes)')
        else:
            typer.echo(f'- Size: {ByteSize.validate(stat["Size"]).human_readable()} ({stat["Size"]} bytes)')
        if stat.get('LastModified'):
            typer.echo(f'- LastModified: {stat["LastModified"]}')
        if stat.get('ETag'):
            typer.echo(f'- ETag: {stat["ETag"]}')
        typer.echo('---' * 10)
        # typer.echo(f'Stats: {stat}')

    typer.echo(f'Total files: {len(stats)}, Total size: {total_size_pretty} ({total_size} bytes) (retrieved in {timer(t):.2f} secs)')
    return stats


async def list_dir(
    source: str,
    pattern: Optional[str] = '*',
) -> List[File]:
    """
    Lists a directory using FileIO backend
    """

    source_path = File(source)
    if not await source_path.async_exists():
        raise FileNotFoundError(f'File {source_path} does not exist')
    if not await source_path.async_is_dir():
        typer.echo(f'File {source_path} is not a directory. Listing parent directory instead')
        source_path = source_path.parent
    
    t = timer()
    files: List[File] = list(await source_path.async_glob(pattern))
    
    for file in files:
        kind = 'File' if await file.async_is_file() else 'Directory'
        typer.echo(f'- {file} ({kind})')
    
    typer.echo(f'Total Files: {len(files)} (retrieved in {timer(t):.2f} secs)')
    return files
    

async def source_exists(
    source: str,
) -> bool:
    """
    Checks if a file or directory exists using FileIO backend
    """

    source_path = File(source)
    exists = await source_path.async_exists()
    typer.echo(f'File {source_path} {"exists" if exists else "does not exist"}')
    return exists



async def rm_files_or_dir(
    source: str,
    recursive: Optional[bool] = False,
    force: Optional[bool] = False,
) -> None:
    """
    Removes a file or directory using FileIO backend
    """

    source_path = File(source)
    if not await source_path.async_exists():
        raise FileNotFoundError(f'File {source_path} does not exist')
    
    t = timer()
    if await source_path.async_is_dir():
        source_files = await source_path.async_ls(
            recursive = recursive, 
            detail = False,
            prettify = False,
            as_path = True,
            files_only = True,
        )
        typer.echo(f'Removing directory {source_path} with {len(source_files)} files (recursive: {recursive}, force: {force})')
        n_removed = 0
        if not force:
            typer.echo('Force is False. Will prompt for confirmation for each file. Use --force to skip confirmation')
        for source_file in source_files:
            if not force and not typer.confirm(f'Remove file {source_file}?'):
                continue
            await source_file.async_rm_file()
            n_removed += 1
        typer.echo(f'Removed {n_removed}/{len(source_files)} files in {timer(t):.2f} secs')

    
    else:
        await source_path.async_rm_file()
        typer.echo(f'Removed file {source_path} in {timer(t):.2f} secs')

# encode = lambda x: base64.b64encode(x).decode()

_supported_encodings: Dict[str, Dict[str, Union[str, Callable]]] = {
    'base64': {
        'encode': lambda x: base64.b64encode(x).decode('utf-8'),
        'mode': 'rb',
        'binary': True,
        'results': b'',
    }
}

_supported_decodings: Dict[str, Dict[str, Union[str, Callable]]] = {
    'base64': {
        'decode': lambda x: base64.b64decode(x),
        'mode': 'r',
        'binary': False,
        'results': '',
    }
}

async def cat_file(
    source: str,
    binary: Optional[bool] = False,
    encode: Optional[str] = None,
    decode: Optional[str] = None,
) -> Union[str, bytes]:
    """
    Extracts the contents of a file using FileIO backend
    """
    source_path = File(source)
    if not await source_path.async_exists():
        raise FileNotFoundError(f'File {source_path} does not exist')
    if not await source_path.async_is_file():
        raise FileNotFoundError(f'File {source_path} is not a file')
    
    if encode:
        assert encode in _supported_encodings, f'Encoding {encode} not supported: {_supported_encodings.keys()}'
        post_func = _supported_encodings[encode]['encode']
        mode = _supported_encodings[encode]['mode']
        results = _supported_encodings[encode]['results']
    elif decode:
        assert decode in _supported_decodings, f'Decoding {decode} not supported: {_supported_decodings.keys()}'
        post_func = _supported_decodings[decode]['decode']
        mode = _supported_decodings[decode]['mode']
        results = _supported_decodings[decode]['results']

    else:
        post_func = None
        mode = 'rb' if binary else 'r'
        results = b'' if binary else ''
    async with source_path.async_open(mode = mode) as f:
        async for line in f:
            results += line
    
    if post_func: results = post_func(results)
    typer.echo(results)
    return results