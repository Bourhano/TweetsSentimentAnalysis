import json
import time

import numpy as np
from kafka import KafkaConsumer
import matplotlib.pyplot as plt


in_topic_name = "all_sentiment_ingest"

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="trend_sentiment",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))


# Set this to True to turn on drift detection mechanisms
monitor_drift = False
window = 20
moving_sentiments = []
drift_threshold = 0.4

print(f"Showing the results of the study on a window size of {window}")
for message in consumer:
    tweet_dict = message.value
    tweet_sentiment = float(tweet_dict['sentiment'])
    tweet_text = tweet_dict["text"]

    if len(moving_sentiments) > window:
        moving_sentiments = moving_sentiments[1:]

        if monitor_drift and np.abs(np.mean(moving_sentiments) - tweet_sentiment):
            # in this concept, the drift is considered if the sentiments shifted
            # by a value of 0.4 from the mean sentiment analyzed.
            print("Drift detected. Resetting our study window.")

            # in this case, we should reset our sliding window.
            moving_sentiments = []

    moving_sentiments.append(tweet_sentiment)
    print(f"Sentiment trend: {np.mean(moving_sentiments)}, current tweet sentiment: {tweet_sentiment}")
    print(tweet_text, '\n')
    plt.plot(moving_sentiments)
    plt.ylim(0, 1)
    plt.show()

    time.sleep(0.2)
