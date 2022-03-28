from kafka import KafkaProducer, KafkaConsumer
import json

in_topic_name = "tweet_ingest"
out_topic_name = "text_hashtag_ingest"

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="get_tweets",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

print(f"Receiving tweets from topic {in_topic_name}")
print(f"Sending tweets to topic {out_topic_name}")

for message in consumer:
    tweet_text = message.value['text']
    tweet_hashtags = message.value['hashtags']

    list_hashtags = []
    for hashtag in tweet_hashtags:
        tag = hashtag['text']
        tag = tag.lower()
        list_hashtags.append(tag)

    set_hashtags = list(set(list_hashtags))
    has_hashtag = len(set_hashtags) > 0
    out_message = {'text': tweet_text,
                   'contains_hashtag': has_hashtag,
                   'hashtag_list': set_hashtags}

    producer.send(out_topic_name, out_message)
