from satyr.multiprocess import create_satyr, SatyrAsyncResult

from dask.context import _globals
from dask.compatibility import Queue
from dask.multiprocessing import _process_get_id
from dask.async import get_async
import os


def get_satyr():
    if 'satyr' not in _globals:
        # TODO manage configurations better
        config = {'user': os.getenv('DEBAS_USER', 'root'),
                  'permanent': True,
                  'filter_refuse_seconds': 1,
                  'max_tasks': 10}

        # TODO question: is it really ok to store this in _globals?
        _globals['satyr'] = create_satyr(config)

    return _globals['satyr']


def apply_async_wrapper(fn, *args, **kwargs):
    """We only send the pickled function defined by the user to
    the Mesos executor; so we have to run the async.execute_task
    beforehand since it's not quiet pickle-able w/ all it's
    arguments. (Especially the queue.)"""

    def wrap(func, *args, **kwargs):
        return get_satyr().apply_async(func, args=args, kwargs=kwargs)

    def resolve_arguments(values=(), asyncs={}):
        resolver = lambda asy, v: asy[v].get() if v in asy else v
        return tuple([resolver(asyncs, val) for val in values])

    func_tuple = kwargs['args'][1]
    kwargs['args'][1] = wrap(func_tuple[0], *resolve_arguments(func_tuple[1:], kwargs['args'][2]))
    fn(*kwargs['args'])


def get(dsk, keys, **kwargs):
    def resolve(res):
        """We'lll get the final result here as an AsyncResult
        which we have to resolve to be compatible w/ other
        dask schedulers. (We shouldn't really force users to
        call .get() after .calculate().) But note that we
        could look for a better resolution than this."""
        return res.get() if not hasattr(res, '__iter__') else (resolve(res[0]),)

    satyr = get_satyr()
    r = resolve(get_async(apply_async_wrapper, satyr.sched.config['max_tasks'], dsk,
                          keys, queue=Queue(), get_id=_process_get_id, **kwargs))

    # TODO force satyr devs to implement a better way to kill
    #      their shitty scheduler from the outside :)
    satyr.sched.driver_states['force_shutdown'] = True

    return r
