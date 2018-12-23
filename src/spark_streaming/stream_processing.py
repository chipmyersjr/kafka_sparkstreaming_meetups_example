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
    top_5_events(rsvps=rsvps)

    ssc.start()
    ssc.awaitTermination()


def count_by_responses(rsvps):
    def update_function(new_values, running_count):
        if running_count is None:
            running_count = 0
        return sum(new_values, running_count)

    def send_updated_results_to_redis(rdd):
        response_count_json = json.dumps(rdd.collectAsMap())
        redis_conn = get_redis_connection()
        redis_conn.set('CountByResponse', response_count_json)

    responses = rsvps.map(lambda rsvp: rsvp.get("response") or "invalid") \
                     .filter(lambda response: response != 'invalid') \
                     .map(lambda response: (response, 1))

    response_counts = responses.updateStateByKey(update_function)
    response_counts.foreachRDD(send_updated_results_to_redis)

    return None


def top_5_events(rsvps):
    def parse_event(rsvp):
        try:
            return rsvp['event']['event_name']
        except (KeyError, UnicodeDecodeError):
            return "invalid"

    def send_top_5_events_to_redis(rdd):
        event_count_json = rdd.collectAsMap()
        iterator = iter(event_count_json.items())
        result_dict = {}
        for _ in range(5):
            try:
                next_result = next(iterator)
                result_dict[next_result[0]] = next_result[1]
            except StopIteration:
                return result_dict

        redis_conn = get_redis_connection()
        redis_conn.set('Top5Events', json.dumps(result_dict))

    responses = rsvps.map(lambda rsvp: parse_event(rsvp)) \
                     .filter(lambda response: response != 'invalid') \
                     .map(lambda response: (response, 1)) \
                     .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 180, 5) \
                     .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    responses.foreachRDD(send_top_5_events_to_redis)

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

