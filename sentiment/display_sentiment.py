from kafka import KafkaConsumer
import json
import numpy as np
import matplotlib.pyplot as plt


in_topic_name = "tweets_sentiment"

consumer = KafkaConsumer(in_topic_name, bootstrap_servers="localhost:9092",
                         group_id="get_sentiments",
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

window = 5
moving_sentiments = []
# drift =
for message in consumer:
    sentiment = float(message.value)

    if len(moving_sentiments) > window:
        moving_sentiments = moving_sentiments[1:]

    moving_sentiments.append(sentiment)
    print(np.mean(moving_sentiments), sentiment)
    plt.plot(moving_sentiments)
    plt.ylim(0, 1)
    plt.show()
