from __future__ import absolute_import, division, print_function

from collections import Iterator

from dask.delayed import (Delayed, DelayedLeaf, flat_unique, funcname,
                          to_task_dasks, tokenize, unzip)
from satyr.proxies.messages import Cpus, Disk, Mem
from toolz import curry, merge

from .executor import get


def to_params(expr, **kwargs):
    if isinstance(expr, MesosDelayed):
        return expr._params
    if isinstance(expr, (Iterator, list, tuple, set)):
        params = [to_params(e) for e in expr]
        return flat_unique(params)
    if isinstance(expr, dict):
        params = [to_params(e) for e in expr.values()]
        return flat_unique(params)
    return []


def to_task_dasks_params(expr, **kwargs):
    task, dasks = to_task_dasks(expr, **kwargs)
    params = to_params(expr, **kwargs)
    return task, dasks, params


@curry
def mesos(obj, name=None, pure=True, cpus=1, mem=64, disk=0, **kwargs):
    kwargs['resources'] = [Cpus(cpus), Mem(mem), Disk(disk)]

    if isinstance(obj, MesosDelayed):
        return obj

    task, dasks, params = to_task_dasks_params(obj)

    if not dasks:
        return MesosDelayedLeaf(obj, pure=pure, name=name, **kwargs)
    else:
        if not name:
            name = '%s-%s' % (type(obj).__name__, tokenize(task, pure=pure))
        dasks.append({name: task})
        params.append({name: kwargs})
        return MesosDelayed(name, dasks, params)


class MesosDelayed(Delayed):

    __slots__ = ('_key', '_dasks', '_params')
    _default_get = staticmethod(get)

    def __init__(self, name, dasks, params):
        super(MesosDelayed, self).__init__(name=name, dasks=dasks)
        object.__setattr__(self, '_params', params)

    def __getstate__(self):
        return (self._key, self._dasks, self._params)

    @property
    def params(self):
        return merge(*self._params)

    def __getattr__(self, attr):
        if not attr.startswith('_'):
            return mesos(getattr, pure=True)(self, attr)
        else:
            raise AttributeError("Attribute {0} not found".format(attr))

    def __call__(self, *args, **kwargs):
        pure = kwargs.pop('pure', False)
        name = kwargs.pop('dask_key_name', None)
        func = mesos(apply, pure=pure)
        if name is not None:
            return func(self, args, kwargs, dask_key_name=name)
        return func(self, args, kwargs)

    def compute(self, **kwargs):
        params = kwargs.pop('params', self.params)
        return super(MesosDelayed, self).compute(params=params, **kwargs)


class MesosDelayedLeaf(DelayedLeaf, MesosDelayed):

    def __init__(self, obj, name=None, pure=False, **params):
        super(MesosDelayedLeaf, self).__init__(obj, name=None, pure=False)
        object.__setattr__(self, '_params', [{self._key: params}])

    def __call__(self, *args, **kwargs):
        params = to_params(args)

        dask_key_name = kwargs.pop('dask_key_name', None)
        pure = kwargs.pop('pure', self.pure)

        if dask_key_name is None:
            name = (funcname(self._data) + '-' +
                    tokenize(self._key, *args, pure=pure, **kwargs))
        else:
            name = dask_key_name

        args, dasks, params = unzip(map(to_task_dasks_params, args), 3)
        if kwargs:
            dask_kwargs, dasks2, params2 = to_task_dasks_params(kwargs)
            params = params + (params2,)
            dasks = dasks + (dasks2,)
            task = (apply, self._data, list(args), dask_kwargs)
        else:
            task = (self._data,) + args

        dasks = flat_unique(dasks)
        dasks.append({name: task})

        params = flat_unique(params)
        params.append({name: self.params[self._key]})
        return MesosDelayed(name, dasks, params)
