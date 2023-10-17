from __future__ import annotations

"""
FileIO CLI
"""

import typer
import asyncio

from typing import Optional

_help = """
FileIO CLI

Usage:
    fileio <command> [options]

Commands:
    copy: Copy a file
    info: Get information about a file or directory
    ls: List files in a directory
    rm: Remove a file or directory
    cat: Echo the file contents

"""

cmd = typer.Typer(no_args_is_help = True, help = _help)

@cmd.command('copy', help = 'Copy a file')
def copy_file(
    source: str = typer.Argument(..., help = 'Path to file to copy'),
    destination: str = typer.Argument(..., help = 'Path to destination file. Can be a directory or a file'),
    filename: Optional[str] = typer.Option(None, help = 'Name of file to upload as. If not provided, will use the name of the source file'),
    new_suffix: Optional[str] = typer.Option(None, help = 'If provided, will replace the suffix of the source file with this suffix'),
    overwrite: Optional[bool] = typer.Option(False, help = 'Whether to overwrite the destination file if it exists'),
):
    """
    Usage:
    Upload or Download a file using FileIO backend

    $ fileio get <source> <destination> [options]

    """
    from .functions import fetch_file
    asyncio.run(
        fetch_file(
            source = source,
            destination = destination,
            filename = filename,
            new_suffix = new_suffix,
            overwrite = overwrite,
        )
    )


@cmd.command('info', help = 'Get information about a file or directory')
def source_info_cmd(
    source: str = typer.Argument(..., help = 'Path to file or directory'),
    recursive: Optional[bool] = typer.Option(False, help = 'Whether to recursively list files'),
    dirs_only: Optional[bool] = typer.Option(False, help = 'Whether to only dirs (default is files only)'),
):
    """
    Usage:
    Get information about a file or directory using FileIO backend

    $ fileio info <source> [options]

    """
    from .functions import source_info
    asyncio.run(
        source_info(
            source = source,
            recursive = recursive,
            dirs_only = dirs_only,
        )
    )


@cmd.command('ls', help = 'List files in a directory')
def list_files(
    source: str = typer.Argument(..., help = 'Path to directory'),
    pattern: str = typer.Argument('*', help = 'Pattern to match files against'),
):
    """
    Usage:
    List files in a directory using FileIO backend

    $ fileio ls <source> [options]

    """
    from .functions import list_dir
    asyncio.run(
        list_dir(
            source = source,
            pattern = pattern,
        )
    )

@cmd.command('rm', help = 'Remove a file or directory')
def rm_cmd(
    source: str = typer.Argument(..., help = 'Path to file or directory'),
    recursive: Optional[bool] = typer.Option(False, help = 'Whether to recursively delete files'),
    force: Optional[bool] = typer.Option(False, help = 'Whether to force delete without prompting'),
):
    """
    Usage:
    Delete a file or directory using FileIO backend

    $ fileio rm <source> [options]

    """
    from .functions import rm_files_or_dir
    asyncio.run(
        rm_files_or_dir(
            source = source,
            recursive = recursive,
            force = force,
        )
    )


@cmd.command('cat', help = 'Echo the file contents')
def cat_cmd(
    source: str = typer.Argument(..., help = 'Path to file'),
    binary: Optional[bool] = typer.Option(False, help = 'Whether to read the file as binary'),
    encode: Optional[str] = typer.Option(None, help = 'Encoding to use when reading the file'),
    decode: Optional[str] = typer.Option(None, help = 'Decoding to use when writing the file'),
):
    """
    Usage:
    Echo the file contents using FileIO backend

    $ fileio cat <source> [options]

    """
    from .functions import cat_file
    asyncio.run(
        cat_file(
            source = source,
            binary = binary,
            encode = encode,
            decode = decode,
        )
    )


if __name__ == '__main__':
    cmd()




