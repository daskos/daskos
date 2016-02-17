#!/usr/bin/env python

from os.path import exists
from setuptools import setup


setup(name='dask.mesos',
      version='0.1',
      description='A Mesos executor backend for Dask',
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      url='http://github.com/lensacom/dask.mesos',
      maintainer='Zoltan Nagy',
      maintainer_email='zoltan.nagy@lensa.com',
      license='BSD',
      keywords='mesos dask multiprocessing',

      packages=['dask_mesos'],
      install_requires=['dask', 'satyr'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
      dependency_links=[
          'git+ssh://git@github.com/lensacom/satyr.git#egg=satyr-0.1'],
      zip_safe=False)
