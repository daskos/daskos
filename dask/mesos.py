from satyr.multiprocess import create_satyr, SatyrAsyncResult
from functools import wraps
from toolz import pipe, curry, partial

from dask.optimize import fuse, cull
from dask.context import _globals
from dask.compatibility import Queue
from dask.multiprocessing import _process_get_id
from dask.async import get_async

import os


def get_satyr():
    if 'satyr' not in _globals:
        config = {'user': os.getenv('DEBAS_USER', 'root'),
                  'permanent': True,
                  'filter_refuse_seconds': 1,
                  'max_tasks': 10}
        _globals['satyr'] = create_satyr(config)

    return _globals['satyr']


@curry
def mesos(fn=None, *args, **kwargs):
    resources = {name: value for name, value in kwargs.items() if name in ['cpus', 'mem', 'disk', 'ports']}

    @wraps(fn)
    def wrapped(*args, **kwargs):
        kwargs['resources'] = resources
        args = [(x.get() if isinstance(x, SatyrAsyncResult) else x) for x in args]
        return get_satyr().apply_async(fn, args=args, kwargs=kwargs)

    return wrapped


def custom_apply_async(fn, *args, **kwargs):
    fn(*kwargs['args'])


def get(dsk, keys, optimizations=[], **kwargs):
    dsk2 = fuse(dsk, keys)
    dsk3 = pipe(dsk2, partial(cull, keys=keys), *optimizations)

    result = get_async(custom_apply_async, 4, dsk3, keys, queue=Queue(), get_id=_process_get_id, **kwargs)

    return result
