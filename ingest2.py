from kafka import KafkaConsumer
import json
import time

in_topic_name = 'tweets-ingest-1000'

consumer = KafkaConsumer(
    in_topic_name,
    bootstrap_servers='localhost:9092',
    # auto_offset_reset='latest',
    # enable_auto_commit=True,
    # auto_commit_interval_ms=5000,
    # fetch_max_bytes = 128,
    # max_poll_records = 100,
    group_id="get-all-tweets",
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    # tweets = json.loads(json.dumps(message.value))
    tweets = message.value
    # print(tweets.keys())
    print(tweets['text'], tweets['coordinates'], tweets['place'])
    time.sleep(1)

