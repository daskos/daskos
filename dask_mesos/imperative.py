from __future__ import absolute_import, division, print_function

from dask.imperative import do
from toolz import curry


@curry
def mesos(fn, pure=True, **kwargs):
    setattr(fn, 'mesos_settings', kwargs)

    return curry(do)(fn, pure=pure)
