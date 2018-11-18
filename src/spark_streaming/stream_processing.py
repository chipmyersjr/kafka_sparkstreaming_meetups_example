from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from json.decoder import JSONDecodeError
import redis

KAKFA_MEETUPS_TOPIC = "meetups"
KAFKA_BROKER = "172.20.0.5:9092"
SPARK_STREAMING_CHECKPOINT_DIR = "/home/data"
REDIS_HOST = "172.20.0.7"
REDIS_PORT = "6379"


def main():
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint(SPARK_STREAMING_CHECKPOINT_DIR)
    sc.setLogLevel("OFF")

    kafka_stream = KafkaUtils.createDirectStream(ssc, [KAKFA_MEETUPS_TOPIC], {"metadata.broker.list": KAFKA_BROKER})

    rsvps = kafka_stream.map(lambda message: parse_rsvp(message))

    count_by_responses(rsvps=rsvps)

    ssc.start()
    ssc.awaitTermination()


def count_by_responses(rsvps):
    responses = rsvps.map(lambda rsvp: rsvp.get("response") or "invalid") \
                     .filter(lambda response: response != 'invalid') \
                     .map(lambda response: (response, 1))

    def update_function(new_values, running_count):
        if running_count is None:
            running_count = 0
        return sum(new_values, running_count)

    def send_updated_results_to_redis(rdd):
        response_count_json = json.dumps(rdd.collectAsMap())
        redis_conn = get_redis_connection()
        redis_conn.set('CountByResponse', response_count_json)

    response_counts = responses.updateStateByKey(update_function)
    response_counts.foreachRDD(send_updated_results_to_redis)

    return None


def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT)


def parse_rsvp(message):
    try:
        rsvp = json.loads(message[1])
        _ = rsvp["response"]
    except JSONDecodeError:
        return json.loads("{}")

    return rsvp


if __name__ == "__main__":
    main()

