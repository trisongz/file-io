import os 
import sys
from setuptools import setup, find_packages

root = os.path.abspath(os.path.dirname(__file__))

package_name = "fileio"
packages = find_packages(include=[package_name, "{}.*".format(package_name)])

_locals = {}
with open(os.path.join(package_name, "_version.py")) as fp:
    exec(fp.read(), None, _locals)

version = _locals["__version__"]
binary_names = _locals["binary_names"]

deps = {
    'main': [
        'tqdm',
        'gdown',
        'requests',
        'pyyaml',
        'pysimdjson',
        'tensorflow>=1.15.0',
    ],
    'extras':{
        'gcp': ['google-api-python-client', 'google-compute-engine', 'google-cloud-storage', 'oauth2client'],
        'cloud': ['smart_open[all]']

    }
}


with open(os.path.join(root, 'README.md'), 'rb') as readme:
    long_description = readme.read().decode('utf-8')

setup(
    name="file_io",
    version=version,
    description="file_io",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Tri Songz',
    author_email='ts@growthengineai.com',
    url='http://github.com/trisongz/file_io',
    python_requires='>3.6',
    install_requires=deps['main'],
    extras_require=deps['extras'],
    packages=packages,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
    ],
)