from debas.mesos import get, get_satyr
from debas.imperative import mesos
from dask import set_options
import dask


@mesos
def inc(x):
    return x+1

@mesos
def add(x, y):
    return x+y

@mesos
def mul(x, y):
    return x*y


with set_options(get=get):
    x = inc(666)
    y = mul(666, 777)
    print add(y, x).compute().get()
