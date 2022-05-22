import os 
import sys
from setuptools import setup, find_packages


version = '0.3.0alpha'
binary_names = ['fileio']
pkg_name = 'fileio'

root = os.path.abspath(os.path.dirname(__file__))
packages = find_packages(
        include=[
            pkg_name, "{}.*".format(pkg_name)
        ]
    )


deps = {
    'main': [
        'tqdm',
        'requests',
        'pyyaml',
        'pysimdjson',
        'smart_open[all]',
        #'gdown',
        #'aioaws',
        #'tensorflow>=1.15.0',
    ],
    'extras':{
        'gcp': ['google-api-python-client', 'google-compute-engine', 'google-cloud-storage', 'oauth2client'],

    }
}

with open(os.path.join(root, 'README.md'), 'rb') as readme:
    long_description = readme.read().decode('utf-8')

setup(
    name="file_io",
    version=version,
    description="Deterministic File Lib to make working with Files across Object Storage easier",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Tri Songz',
    author_email='ts@growthengineai.com',
    url='http://github.com/trisongz/file-io',
    python_requires='>=3.6',
    install_requires=deps['main'],
    extras_require=deps['extras'],
    packages=packages,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
    ],
)