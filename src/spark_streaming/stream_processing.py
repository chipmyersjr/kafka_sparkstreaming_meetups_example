from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from json.decoder import JSONDecodeError


def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/home/data")
    sc.setLogLevel("OFF")

    kafka_stream = KafkaUtils.createDirectStream(ssc, ["meetups"], {"metadata.broker.list": "172.20.0.5:9092"})

    rsvps = kafka_stream.map(lambda message: parse_rsvp(message))

    count_by_responses(rsvps=rsvps)

    ssc.start()
    ssc.awaitTermination()


def parse_rsvp(message):
    try:
        rsvp = json.loads(message[1])
        _ = rsvp["response"]
    except JSONDecodeError:
        return json.loads("{}")

    return rsvp


def count_by_responses(rsvps):
    responses = rsvps.map(lambda rsvp: rsvp.get("response") or "invalid") \
                     .filter(lambda response: response != 'invalid') \
                     .map(lambda response: (response, 1))

    def update_function(new_values, running_count):
        if running_count is None:
            running_count = 0
        return sum(new_values, running_count)

    def convert_response_counts_to_dictionary(rdd):
        response_count_dict = rdd.collectAsMap()
        print(response_count_dict)

    response_counts = responses.updateStateByKey(update_function)
    response_counts.foreachRDD(convert_response_counts_to_dictionary)

    return None


if __name__ == "__main__":
    main()

