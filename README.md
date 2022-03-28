# TweetsSentimentAnalysis

A group project implemented with Twitter API, Apache Kafka and Python to ingest tweets and analyse them in order to predict the sentiment of the tweeter.

To run the code **on Windows**:

1. Go to [kafka-path]\kafka_2.12-3.1.0\bin\windows
   where [kafka-path] is the local path to the location that contains your kafka installation.


2. Launch a Terminal in the said folder and run the following line to create another Terminal tab in the same folder:

    $ wt -w 0 nt -d .

3. On the first Terminal tab run the following to start ZooKeeper server

    $ .\zookeeper-server-start.bat ..\..\config\zookeeper.properties

4. On the second Terminal tab run the following to start Kafka:

    $ .\kafka-server-start.bat ..\..\config\server.properties

5. Launch your Python script.
