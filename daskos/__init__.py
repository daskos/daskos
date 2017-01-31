from __future__ import absolute_import, division, print_function

import logging
import pkg_resources as _pkg_resources

from .delayed import mesos
from .executor import MesosExecutor, get

logging.basicConfig(format='%(relativeCreated)6d %(threadName)s %(message)s')

__version__ = _pkg_resources.get_distribution('dask.mesos').version

__all__ = ('MesosExecutor', 'get', 'mesos')
