FROM lensa/satyr:latest

ADD . /dask.mesos
WORKDIR /dask.mesos
RUN pip install .
