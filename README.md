Run dask workflows on your Mesos cluster.

# Example

```python
from dask_mesos.mesos import get
from dask_mesos.imperative import mesos
from dask import set_options

@mesos(cpus=0.5, mem=64)
def inc(x):
    return x+1

@mesos(cpus=0.5, mem=128)
def add(x, y):
    return x+y

@mesos(cpus=1, mem=128)
def mul(x, y):
    return x*y


with set_options(get=get):
    x = inc(66)
    y = mul(66, 77)
    print add(y, x).compute()
```

# Build with Wercker-CLI

```bash
wercker build --working-dir ~/.wercker
```
