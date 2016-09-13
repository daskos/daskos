FROM satyr

# TODO use conda
RUN apk --no-cache add --virtual .build-deps \  
    musl-dev \
    gcc \
    linux-headers \
    python-dev \
 && pip --no-cache-dir install distributed \
 && rm -rf /var/cache/apk/* \
 && rm -rf /tmp/* \
 && apk del .build-deps

ADD . /opt/dask.mesos
RUN pip --no-cache-dir install /opt/dask.mesos 
