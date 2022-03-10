from kafka import KafkaProducer, KafkaConsumer
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.downloader import download


download('vader_lexicon')
print("finished download vader")

in_topic_name = "tweets_ingest"
out_topic_name = "tweets_sentiment"

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="get_tweets",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

sid = SentimentIntensityAnalyzer()

print("starting tweets reading")
for message in consumer:
    print("received tweet")
    tweet = message.value
    print(tweet)
    sentiment = sid.polarity_scores(tweet)
    print("sid succeed")

    sentiment = sentiment['compound']
    sentiment = (sentiment + 1) / 2
    print("sent", end=" ")
    producer.send(out_topic_name, str(sentiment))
