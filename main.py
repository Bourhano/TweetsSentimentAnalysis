# Press the green button in the gutter to run the script.
from sentiment import tweet_ingest, sentiment_analyser
import subprocess

if __name__ == '__main__':
    # subprocess.call("./tweet_ingest.py", shell=True)
    print('Start')
    subprocess.call(tweet_ingest, shell=True)
    subprocess.call(sentiment_analyser, shell=True)
    print("hi")
