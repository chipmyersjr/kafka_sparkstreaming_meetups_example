import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 5)

    kafkaStream = KafkaUtils.createDirectStream(ssc, ["meetups"], {"metadata.broker.list": "172.20.0.5:9092"})

    rsvps = kafkaStream.map(lambda message: json.loads(message[1]))
    rsvps.count().map(lambda x: 'RSVPS in this batch: %s' % x).pprint()

    responses = rsvps.map(lambda rsvp: rsvp['response'])
    responses.pprint()

    ssc.start()
    ssc.awaitTermination()