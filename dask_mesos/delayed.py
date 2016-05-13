from __future__ import absolute_import, division, print_function

from toolz import curry
from dask import delayed
from satyr.proxies.messages import Cpus, Disk, Mem
from .satyr import SatyrPack



@curry
def mesos(obj, name=None, pure=True, cpus=1, mem=64, disk=0, **kwargs):
    kwargs['resources'] = [Cpus(cpus), Mem(mem), Disk(disk)]
    kwargs['name'] = name or 'dask.mesos'
    pack = SatyrPack(fn=obj, params=kwargs)
    return delayed(pack, name=name, pure=pure)
