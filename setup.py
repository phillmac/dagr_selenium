#!/usr/bin/env python

from setuptools import setup, find_packages

version = None
exec(open('dagr_selenium/version.py').read())
with open('README.md', 'r') as fh:
    long_description = fh.read()
setup(
    name='dagr_selenium',
    version=version,
    description='Selenium worker & manager scripts for DAGR Revamped',
    author='Phillip Mackintosh',
    url='https://github.com/phillmac/dagr_selenium',
    packages=find_packages(),
    install_requires=[
        'docopt == 0.6.2',
        'dagr_revamped @ git+https://github.com/phillmac/dagr_revamped@b626ee016c0b42460b5a69e418868e4dd4ad8b7c',
        'selenium==3.141.0',
        'aiofiles==0.6.0',
        'aiohttp==3.7.4.post0'
    ],
    extras_require={
        'calmjs':  ["calmjs==3.3.1"]
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ]
)
