from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

consumer_key = "ljCUp7n4TVv1Ph2KJXyxj23rC"
consumer_secret = "u3xAHSemH3g8t3o2kLRCOHtXfmqanyyyIdfMa2H8L4RqTI0xJT"

access_token = "1445388596714876948-46Vq2TfKnjXgkIGNk95k34r9HuUeIw"
access_token_secret = "v4ikqp5dI6q5Kbmz9Q5ItJHiLD7fKZFGtAPzF8Rfsu0vO"

out_topic_name = "tweets-ingest-1000"


class StdOutListener(StreamListener):
    def on_data(self, data):
        json_ = json.loads(data)
        # producer.send(out_topic_name, json_["text"].encode('utf-8'))
        producer.send(out_topic_name, json_)
        return True

    def on_error(self, status):
        print(status)


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
listener = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listener)
stream.filter(track=["france"], is_async=True)
