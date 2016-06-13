#!/usr/bin/env python
# coding: utf-8

from __future__ import absolute_import, division, print_function

from os.path import exists

from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-v', 'dask_mesos/']
        self.test_suite = True

    def run_tests(self):
        import pytest
        import sys

        errno = pytest.main(self.test_args)
        self.handle_exit()
        sys.exit(errno)

    @staticmethod
    def handle_exit():
        import atexit
        atexit._run_exitfuncs()


setup(name='dask.mesos',
      version='0.2.1',
      description='Apache Mesos backend for Dask scheduling library',
      long_description=(open('README.md').read() if exists('README.md')
                        else ''),
      url='http://github.com/lensacom/dask.mesos',
      maintainer='Krisztián Szűcs',
      maintainer_email='krisztian.szucs@lensa.com',
      license='Apache License, Version 2.0',
      keywords='mesos dask multiprocessing scheduling satyr',
      cmdclass={'test': PyTest},
      packages=['dask_mesos'],
      install_requires=['toolz', 'dask>=0.9.0', 'satyr>=0.2.0'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest-mock', 'pytest'],
      zip_safe=False)
