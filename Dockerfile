FROM bdas-master-3:5000/satyr

ADD . /dask.mesos
WORKDIR /dask.mesos
RUN pip install .
