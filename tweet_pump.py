from utils import read_keys
import json

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer


class StdOutListener(StreamListener):
    def on_data(self, data):
        tweet = json.loads(data)
        tweet_text = tweet["text"]
        tweet_hashtags = tweet['entities']['hashtags']

        out_message = {'text': tweet_text,
                       'hashtags': tweet_hashtags}
        producer.send(out_topic_name, out_message)

        return True

    def on_error(self, status):
        print(status)


out_topic_name = "tweet_ingest"
consumer_key, consumer_secret, access_token, access_token_secret = read_keys()

print(f"Sending tweets to topic {out_topic_name}")

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
listener = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listener)

queries = []
print("Write the desired queries (words or group of words) to be monitored: (enter empty line when done)")
query = input()
while query != '':
    queries.append(query)
    query = input()

if not queries:
    queries = ["april's fool"]
    print(f"\tQuerying for: {queries}")

print("Write the desired languages ex. 'en fr' (for English and French) : ", end=" ")
langs = input().split(' ')
if langs == ['']:
    print("Taking english tweets.")
    langs = ["en"]

print(queries, langs)
print("Pumping started...")
stream.filter(languages=langs, track=queries)
