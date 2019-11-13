FROM python:3.6-alpine3.9

RUN apk add --no-cache --virtual .build alpine-sdk curl-dev libcurl zlib-dev bzip2-dev libbz2 automake autoconf libtool libpthread-stubs && \
    apk add --no-cache libcurl libbz2

RUN cd /tmp && \
    git clone https://github.com/wanduow/wandio.git && \
    git clone https://github.com/CAIDA/bgpstream.git

COPY bgpstream.patch /tmp/bgpstream

RUN cd /tmp/wandio/ \
    && ./bootstrap.sh \
    && ./configure --with-http --with-zlib --with-bzip2 \
    && make \
    && make install ; \
    cd /tmp/bgpstream \
    && patch < bgpstream.patch \
    && ./autogen.sh \
    && ./configure \
    && make \
    && make install ; \
    cd /tmp/bgpstream/pybgpstream && \
    python3 setup.py build && \
    python setup.py install ; \
    rm -rf /tmp/wandio /tmp/bgpstream

RUN cd /tmp && \
    curl -O https://codeload.github.com/edenhill/librdkafka/tar.gz/v1.2.1 && \
    tar -xzvf v1.2.1 && \
    cd librdkafka-1.2.1/ && \
    ./configure && \ 
    make && \
    make install && \
    rm -rf /tmp/librdkafka-1.2.1 /tmp/v1.2.1

RUN pip3 install \
    confluent-kafka \
    arrow \
    msgpack 

RUN apk del .build

RUN mkdir /app && \
    mkdir /app/logs

WORKDIR /app

COPY ./app/*.py /app/

ENTRYPOINT ["/bin/sh"]

