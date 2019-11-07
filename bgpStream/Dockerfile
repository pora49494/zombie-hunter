FROM python

RUN mkdir /src && cd /src && \ 
    curl -O https://research.wand.net.nz/software/wandio/wandio-4.0.0.tar.gz && \
    tar zxf wandio-4.0.0.tar.gz && \
    cd wandio-4.0.0/ && \
    ./configure && \
    make && \
    make install && \
    ldconfig

RUN cd /src && \ 
    curl -O https://bgpstream.caida.org/bundles/caidabgpstreamwebhomepage/dists/bgpstream-1.2.3.tar.gz && \
    tar zxf bgpstream-1.2.3.tar.gz && \
    cd bgpstream-1.2.3 && \
    ./configure && \
    make && \
    make check && \ 
    make install && \ 
    ldconfig

RUN apt-get -y update && \ 
    apt-get -y install software-properties-common

RUN cd /src && \
    curl -O https://codeload.github.com/edenhill/librdkafka/tar.gz/v1.2.1 && \
    tar -xzvf v1.2.1 && \
    cd librdkafka-1.2.1/ && \
    ./configure && \ 
    make && \
    make install && \
    ldconfig

RUN cd /src && \
    git clone https://github.com/CAIDA/bgpstream.git && \
    cd bgpstream/pybgpstream && \
    python3 setup.py build && \
    python setup.py install

RUN pip3 install \
    confluent-kafka \
    arrow \
    msgpack 

RUN mkdir /app 

COPY ./app/bgpProducer.py /app/

ENTRYPOINT ["/bin/bash"]
