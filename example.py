from __future__ import absolute_import, division, print_function

from dask import set_options
from dask_mesos.imperative import mesos
from dask_mesos.mesos import get


@mesos(mem=256, cpus=0.5, image='bdas-master-3:5000/satyr')
def inc(x):
    return x + 1


@mesos(mem=256, cpus=0.5, image='bdas-master-3:5000/satyr')
def add(x, y):
    return x + y


@mesos(mem=256, cpus=0.5, image='bdas-master-3:5000/satyr')
def mul(x, y):
    return x * y


with set_options(get=get):
    one = inc(0)
    alot = add(one, 789)
    gigalot = mul(alot, inc(alot))
    print(mul(alot, gigalot).compute())
