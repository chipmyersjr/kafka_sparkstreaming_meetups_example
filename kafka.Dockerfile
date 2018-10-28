FROM spotify/kafka

RUN apt-get update && apt-get install vim && \

WORKDIR /bin

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py && \
    pip install requests