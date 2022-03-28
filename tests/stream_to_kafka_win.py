consumer_key = "ljCUp7n4TVv1Ph2KJXyxj23rC"
consumer_secret = "u3xAHSemH3g8t3o2kLRCOHtXfmqanyyyIdfMa2H8L4RqTI0xJT"

access_token = "1445388596714876948-46Vq2TfKnjXgkIGNk95k34r9HuUeIw"
access_token_secret = "v4ikqp5dI6q5Kbmz9Q5ItJHiLD7fKZFGtAPzF8Rfsu0vO"

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer


topic_name = "tweets-ingest-2"
producer = KafkaProducer(bootstrap_servers="localhost:9092")


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS()
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["Apple"], stall_warnings=True, languages= ["en"])


class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()
