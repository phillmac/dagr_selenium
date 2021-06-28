#!/usr/bin/env python

from setuptools import setup, find_packages

version = None
exec(open('dagr_selenium/version.py').read())
setup(
    name='dagr_selenium',
    version=version,
    description='Selenium worker & manager scripts for DAGR Revamped',
    author='Phillip Mackintosh',
    url='https://github.com/phillmac/dagr_selenium',
    packages=find_packages(),
    install_requires=[
        'docopt==0.6.2',
        'python-dotenv==0.17.1',
        'dagr_revamped @ git+https://github.com/phillmac/dagr_revamped@0.2.86-dev.0',
        'selenium==3.141.0',
        'aiofiles @ git+https://github.com/phillmac/aiofiles@0.12.0dev0',
        'aiohttp==3.7.4.post0'
    ],
    extras_require={
        'calmjs':  ['calmjs==3.3.1'],
        'easywebdav': ['easywebdav==1.2.0'],
        'full': ['calmjs', 'easywebdav']
    },
    classifiers=[
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ]
)
