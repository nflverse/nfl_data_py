# -*- coding: utf-8 -*-
"""
setup.py

installation script
"""

from setuptools import setup, find_packages

PACKAGE_NAME = "nfl_data_py"


def run():
    setup(name=PACKAGE_NAME,
          version="0.1",
          description="python library for interacting with NFL data sourced from nflfastR",
          author="cooperdff",
          license="MIT",
          packages=find_packages(),
          package_data={'': ['data/*.*']},
          zip_safe=False)


if __name__ == '__main__':
    run()