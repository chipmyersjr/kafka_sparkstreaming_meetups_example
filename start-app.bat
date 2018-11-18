docker-compose up -d

docker exec -d kafka_sparkstreaming_meetups_example_kafka_1 python /home/code/kafka/producer.py

docker exec -d kafka_sparkstreaming_meetups_example_spark_1 python /home/code/spark_streaming/stream_processing.py

docker exec -d kafka_sparkstreaming_meetups_example_django_1 python manage.py runserver