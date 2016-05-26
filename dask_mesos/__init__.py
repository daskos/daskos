from __future__ import absolute_import, division, print_function

import logging

logging.basicConfig(level=logging.INFO,
                    format='%(relativeCreated)6d %(threadName)s %(message)s')

from .satyr import get
from .delayed import mesos
