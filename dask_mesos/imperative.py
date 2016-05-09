from __future__ import absolute_import, division, print_function

from dask import delayed
from satyr.proxies.messages import Cpus, Disk, Mem
from toolz import curry


@curry
def mesos(fn, pure=True, cpus=1, mem=64, disk=0, **kwargs):
    kwargs['resources'] = [Cpus(cpus), Mem(mem), Disk(disk)]
    setattr(fn, 'satyr', kwargs)
    return delayed(fn, pure=pure)
