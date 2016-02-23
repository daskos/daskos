from __future__ import absolute_import, division, print_function

from dask import set_options
from dask.imperative import do
from dask_mesos.imperative import mesos
from dask_mesos.mesos import get


"""Run stuff on mesos w/o docker but w/ resources set."""
@mesos(mem=512, cpus=1)
def inc(x):
    return x + 1


"""Run stuff on mesos w/o docker and w/ default resource setup."""
@do
def add(x, y):
    return x + y


"""Run stuff on mesos w/ docker and specified resources."""
@mesos(mem=512, cpus=1, image='bdas-master-3:5000/satyr')
def mul(x, y):
    return x * y


"""This context ensures that @do will also run on mesos."""
with set_options(get=get):
    one = inc(0)
    alot = add(one, 789)
    gigalot = mul(alot, inc(alot))
    print(mul(alot, gigalot).compute())
