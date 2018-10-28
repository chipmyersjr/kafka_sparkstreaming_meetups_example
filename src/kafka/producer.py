import requests
from kafka import KafkaProducer


class Producer:
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def run(self):
        for rsvp in self.stream_meetup_initial():
            self.producer.send('meetups', rsvp)

    def stream_meetup_initial(self):
        uri = "http://stream.meetup.com/2/rsvps"
        response = requests.get(uri, stream=True)
        for chunk in response.iter_content(chunk_size=None):
            yield chunk


if __name__ == "__main__":
    p = Producer()
    p.run()
