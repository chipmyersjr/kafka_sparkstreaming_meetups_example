from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from json.decoder import JSONDecodeError


def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 5)

    kafkaStream = KafkaUtils.createDirectStream(ssc, ["meetups"], {"metadata.broker.list": "172.20.0.5:9092"})

    rsvps = kafkaStream.map(lambda message: parse_rsvp(message))
    rsvps.count().map(lambda x: 'RSVPS in this batch: %s' % x).pprint()

    responses = rsvps.map(lambda rsvp: rsvp.get("response") or "invalid").filter(lambda response: response != 'invalid')
    responses.pprint()

    ssc.start()
    ssc.awaitTermination()


def parse_rsvp(message):
    try:
        rsvp = json.loads(message[1])
        _ = rsvp["response"]
    except JSONDecodeError:
        return json.loads("{}")

    return rsvp


if __name__ == "__main__":
    main()

