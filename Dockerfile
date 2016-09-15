FROM lensa/satyr

RUN conda install -y nomkl dask distributed==1.13.2 \
 && conda clean -a

ADD . /opt/dask.mesos
RUN pip --no-cache-dir install /opt/dask.mesos
