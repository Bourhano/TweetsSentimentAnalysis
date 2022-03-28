import time
from kafka import KafkaProducer
import requests
import json
import tweepy
from tweepy.streaming import Stream
from tweepy import OAuthHandler


consumer_key = "ljCUp7n4TVv1Ph2KJXyxj23rC"
consumer_secret = "u3xAHSemH3g8t3o2kLRCOHtXfmqanyyyIdfMa2H8L4RqTI0xJT"

access_token = "1445388596714876948-46Vq2TfKnjXgkIGNk95k34r9HuUeIw"
access_token_secret = "v4ikqp5dI6q5Kbmz9Q5ItJHiLD7fKZFGtAPzF8Rfsu0vO"

topic_name = "tweets-ingest"
producer = KafkaProducer(bootstrap_servers="localhost:9092")


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self, ):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth


class TwitterStreamer:

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            # listener = ListenerTS(consumer_key, consumer_secret, access_token, access_token_secret)
            # listener = MyListener(consumer_key, consumer_secret, access_token, access_token_secret)
            listener = MyListener()
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = tweepy.Stream(auth, listener, access_token, access_token_secret)
            stream.filter(track=["Apple"], languages=["en"])


class TweetListener (tweepy.Stream):
    def __init__(self):
        super(tweepy.Stream).__init__()


class MyListener(TweetListener):
    def on_data(self, data):
        try:
            with open('python.json', 'a') as f:
                f.write(data)
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()

# auth = tweepy.AppAuthHandler(consumerKey, consumerKeySecret)
# api = tweepy.API(auth)
# for tweet in tweepy.Cursor(api.search_tweets, q='Ukraine').items(5):
#     print(tweet.text)

# i = 0
# while True:
#     i += 1
#     response = requests.get("https://api.jcdecaux.com/vls/v1/stations?apiKey="+api_key)
#     message = response.json()
#
#     producer.send(topic_name,  message)
#     print(f"Sending message {i} to topic: {topic_name}")
#
#     # print(message[0].keys())
#     time.sleep(8)
