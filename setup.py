#!/usr/bin/env python
# coding: utf-8

from os.path import exists

from setuptools import setup

setup(name='dask.mesos',
      version='0.2.2',
      description='Apache Mesos backend for Dask scheduling library',
      long_description=(open('README.md').read() if exists('README.md')
                        else ''),
      url='http://github.com/lensacom/dask.mesos',
      maintainer='Krisztián Szűcs',
      maintainer_email='krisztian.szucs@lensa.com',
      license='Apache License, Version 2.0',
      keywords='mesos dask multiprocessing scheduling satyr',
      packages=['dask_mesos', 'dask_mesos.distributed'],
      install_requires=['toolz', 'dask>=0.9.0', 'satyr>=0.2.0'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest-mock', 'pytest'],
      zip_safe=False)
