#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from setuptools import find_packages, setup

# Package metadata
NAME = 'nfl_data_py'
DESCRIPTION = 'python library for interacting with NFL data sourced from nflfastR'
URL = 'https://github.com/nflverse/nfl_data_py'
EMAIL = 'alec.ostrander@gmail.com'
AUTHOR = 'Alec Ostrander'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.3.3'


# What packages are required for this module to be executed?
REQUIRED = [
    'numpy>=1.0, <2.0',
    'pandas>=1.0, <2.0',
    'appdirs>1',
    'fastparquet>0.5',
]

# What packages are optional?
EXTRAS = {
}

# Where the magic happens:
setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=Path('README.md').read_text(),
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*", "tests.*"]),
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    include_package_data=True,
    license='MIT',
    classifiers=[
        # Trove classifiers. Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        *[f'Programming Language :: Python :: 3.{i}' for i in range(6, 13)],
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Operating System :: OS Independent',
    ],
)
