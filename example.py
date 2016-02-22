from __future__ import absolute_import, division, print_function

from dask import set_options
from dask.imperative import do
from dask_mesos.imperative import mesos
from dask_mesos.mesos import get


@mesos(mem=512, cpus=1, image='bdas-master-3:5000/satyr')
def inc(x):
    return x + 1


@mesos(mem=512, cpus=1, image='bdas-master-3:5000/satyr')
def add(x, y):
    return x + y


@mesos(mem=512, cpus=1, image='bdas-master-3:5000/satyr')
def mul(x, y):
    return x * y


with set_options(get=get):
    one = inc(0)
    alot = add(one, 789)
    print(mul(alot, 987).compute())
