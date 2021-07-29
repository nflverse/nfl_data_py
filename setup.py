from setuptools import setup

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
	name='nfl_data_py',
	version='0.1.0',
	description='Package for working with NFL data',
    author='cooperdff',
    author_email='cooper.dff11@gmail.com',
    url='https://github.com/cooperdff/nfl_data_py',
    license='MIT',
    classifiers=[
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
],
	packages=['nfl_data_py'],
	package_dir={'nfl_data_py': ''},
    python_requires='>=3.6',
    install_requires=[
        'numpy>1',
        'pandas>1',
        'datetime>3.5',
        'fastparquet>0.5',
        'python-snappy>0.5',
        'snappy>1',
    ],
    long_description=long_description,
    long_description_content_type='text/markdown'
)