# TweetsSentimentAnalysis

A group project implemented with Twitter API, Apache Kafka and Python to ingest tweets and analyse them in order to predict the sentiment of the tweeter.


**Important**: the file ``keys.txt`` should contain the Twitter API keys in the same order specified in the said file. Note that you should also rename the corresponding variable in the first script of step  5. (read below)

**Requirements**:
- working kafka installation
- nltk library
- tweepy library

To run the code **on Windows**:

1. Go to [kafka-path]\kafka_2.12-3.1.0\bin\windows
   where [kafka-path] is the local path to the location that contains your kafka installation.


2. Launch a Terminal in the said folder and run the following line to create another Terminal tab in the same folder:

    $ wt -w 0 nt -d .

3. On the first Terminal tab run the following to start ZooKeeper server

    $ .\zookeeper-server-start.bat ..\..\config\zookeeper.properties

4. On the second Terminal tab run the following to start Kafka:

    $ .\kafka-server-start.bat ..\..\config\server.properties

5. Launch the scripts in the following order:
   - tweet_pump.py
   - text_hashtag_pump.py
   - sentiment_analyzer.py
   - sentiment_separator.py
   - trend_stats.py
   - trend_negative_hashtags.py
   - trend_positive_hashtags.py
   - trend_sentiments.py

The best practice is to have a separate shell for every script, since it is an online application.
