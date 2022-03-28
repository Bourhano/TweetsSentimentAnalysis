import json
from kafka import KafkaConsumer, KafkaProducer


in_topic_name = "hashtag_sentiment_ingest"
out_topic_names = {"pos": "positive_sentiment",
                   "neg": "negative_sentiment"}

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="sentiment_separator",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

pos_producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

neg_producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

print(f"Receiving tweets with sentiments from topic {in_topic_name}")
print(f"Sending positive analyzed tweets to topic {out_topic_names['pos']}")
print(f"Sending negative analyzed tweets to topic {out_topic_names['neg']}")

for message in consumer:
    tweet_dict = message.value
    sentiment = float(tweet_dict['sentiment'])

    if sentiment > 0.7:
        print(f"Sending positive tweet to {out_topic_names['pos']}")
        pos_producer.send(out_topic_names["pos"], tweet_dict)

    elif sentiment < 0.3:
        print(f"Sending negative tweet to {out_topic_names['neg']}")
        neg_producer.send(out_topic_names["neg"], tweet_dict)
