FROM spotify/kafka

RUN apt-get update && apt-get install -y vim

WORKDIR /bin

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py && \
    pip install requests && pip install kafka

RUN nohup bash -c "/home/code/kafka/start_producer.sh &" && sleep 4