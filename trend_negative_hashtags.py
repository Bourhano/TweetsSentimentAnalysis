import json
import time

from kafka import KafkaConsumer

CYCLES = 100
in_topic_name = "negative_sentiment"

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="negative_trends",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))


print("Printing hashtags correlated with the most negative tweets.")

cycles = 0
hashtags_dict = {}
for message in consumer:
    tweet_dict = message.value
    sentiment = float(tweet_dict['sentiment'])
    tweet_hashtags = tweet_dict['hashtag_list']
    tweet_text = tweet_dict['text']

    for hashtag in tweet_hashtags:
        if hashtag in hashtags_dict:
            hashtags_dict[hashtag] += 1
        else:
            hashtags_dict[hashtag] = 1

    worst_hashtag = max(hashtags_dict, key=hashtags_dict.get)
    print(f"{worst_hashtag:20}: {hashtags_dict[worst_hashtag]}")

    cycles += 1
    if cycles == CYCLES:
        cycles = 0
        print("\nStats for the top 5 negative-related hashtags:")
        print(dict(sorted(hashtags_dict.iteritems(), key=hashtags_dict.get, reverse=True)[:5]))
        print(f"\nAn example tweet: {tweet_text}")

    # time.sleep(0.2)
