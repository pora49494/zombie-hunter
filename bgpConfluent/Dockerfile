FROM python

RUN apt-get -y update && \ 
    apt-get -y install software-properties-common

RUN mkdir /src && \
    cd /src && \
    curl -O https://codeload.github.com/edenhill/librdkafka/tar.gz/v1.2.1 && \
    tar -xzvf v1.2.1 && \
    cd librdkafka-1.2.1/ && \
    ./configure && \ 
    make && \
    make install && \
    ldconfig

RUN pip3 install \ 
    confluent-kafka \
    arrow \
    msgpack 

RUN mkdir /app 

COPY ./app/bgpAnalyzer.py ./app/bgpScheduler.py ./app/zombieDetector.py /app/

ENTRYPOINT ["/bin/bash"]