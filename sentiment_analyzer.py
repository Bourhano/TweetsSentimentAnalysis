from kafka import KafkaProducer, KafkaConsumer
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.downloader import download

print("Getting required packages...")
download('vader_lexicon')
print("Packages ready.\n")

in_topic_name = "text_hashtag_ingest"

out_topic_name_all = "all_sentiment_ingest"
out_topic_name_hashtag = "hashtag_sentiment_ingest"

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="get_extracted_tweets",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

producer_all = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

producer_hashtag = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

sid = SentimentIntensityAnalyzer()

print("Starting tweets analysis...")
for message in consumer:
    tweet_dict = message.value
    tweet_text = tweet_dict['text']
    has_tag = tweet_dict['contains_hashtag']

    sentiment = sid.polarity_scores(tweet_text)
    sentiment = sentiment['compound']
    sentiment = (sentiment + 1) / 2

    tweet_dict['sentiment'] = sentiment
    if has_tag:
        producer_hashtag.send(out_topic_name_hashtag, tweet_dict)
    producer_all.send(out_topic_name_all, tweet_dict)
