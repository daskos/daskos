from toolz import curry
from dask.imperative import do


@curry
def mesos(fn, pure=True, **kwargs):
    setattr(fn, 'mesos_settings', kwargs)

    return curry(do)(fn, pure=pure)
