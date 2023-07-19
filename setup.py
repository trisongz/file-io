import sys
from pathlib import Path
from setuptools import setup, find_packages

if sys.version_info.major != 3:
    raise RuntimeError("This package requires Python 3+")

pkg_name = 'file-io'
gitrepo = 'trisongz/file-io'

root = Path(__file__).parent
version = root.joinpath('fileio/version.py').read_text().split('VERSION = ', 1)[-1].strip().replace('-', '').replace("'", '')


requirements = [
    'anyio',
    'aiofile',
    #'aiopath', # remove deps as 3.10 vs 3.9 is different
    'fsspec',
    'loguru',
    'pydantic',
    'dill',
    'frozendict',
    'aiohttpx',
    'typer',
]

extras = {
    'gcs': ['gcsfs'],
    's3': ['s3fs'],
    'cloud': ['gcsfs', 's3fs'],
}

# pip install fileio[cloud]

args = {
    'packages': find_packages(include = ['fileio', 'fileio.*']),
    'install_requires': requirements,
    'include_package_data': True,
    'long_description': root.joinpath('README.md').read_text(encoding='utf-8'),
    'entry_points': {
        'console_scripts': [
            'fileio = fileio.cli:cmd',
        ],
    }
}

if extras: args['extras_require'] = extras

setup(
    name = pkg_name,
    version = version,
    url=f'https://github.com/{gitrepo}',
    license='MIT Style',
    description='Deterministic File Lib to make working with Files across Object Storage easier',
    author='Tri Songz',
    author_email='ts@growthengineai.com',
    long_description_content_type="text/markdown",
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries',
    ],
    **args
)
