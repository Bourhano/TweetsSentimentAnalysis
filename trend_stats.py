import json
import time

from kafka import KafkaConsumer

CYCLES = 20
in_topic_name = "all_sentiment_ingest"

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="stats",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

cycles = 0
tweet_counts = 0
tweet_with_hashtag_counts = 0
total_hashtags = 0
average_sentiment = 0
old_time = time.time()
print("Printing periodical stats about studied tweet topics...")
for message in consumer:
    cycles += 1
    tweet_dict = message.value
    tweet_sentiment = float(tweet_dict['sentiment'])
    tweet_has_tag = tweet_dict['contains_hashtag']
    tweet_hashtags = tweet_dict['hashtag_list']

    if tweet_has_tag:
        tweet_with_hashtag_counts += 1
        total_hashtags += len(tweet_hashtags)
    average_sentiment = average_sentiment * tweet_counts + tweet_sentiment
    tweet_counts += 1
    average_sentiment /= tweet_counts

    if cycles == CYCLES:
        new_time = time.time()
        delta = new_time - old_time
        print("Total number of tweets received           :", tweet_counts)
        print("Total number of hashtags received         :", total_hashtags)
        print("Total number of tweets containing hashtags:", tweet_with_hashtag_counts)
        print("Average tweets sentiment for studied topic:", average_sentiment)
        print("Tweets frequency on defined topics        :", cycles / delta, "tweet/s\n")

        cycles = 0
        old_time = new_time
