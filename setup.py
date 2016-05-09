#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function

from os.path import exists
from setuptools import setup


setup(name='dask.mesos',
      version='0.1.3',
      description='Apache Mesos backend for Dask scheduling library',
      long_description=(open('README.md').read() if exists('README.md')
                        else ''),
      url='http://github.com/lensacom/dask.mesos',
      maintainer='Krisztián Szűcs',
      maintainer_email='krisztian.szucs@lensa.com',
      license='Apache License, Version 2.0',
      keywords='mesos dask multiprocessing scheduling satyr',
      packages=['dask_mesos'],
      install_requires=['toolz', 'dask', 'satyr'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      zip_safe=False)
