from __future__ import absolute_import, division, print_function

from dask import set_options
from dask.imperative import do
from dask_mesos.imperative import mesos
from dask_mesos.mesos import get


@do
def inc(x):
    return x + 1


@mesos(cpus=1)
def add(x, y):
    return x + y


@mesos(mem=64, image='bdas-master-3:5000/satyr')
def mul(x, y):
    return x * y


with set_options(get=get):
    print(mul(789, 987).compute())
