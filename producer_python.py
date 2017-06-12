#!/usr/bin/env python
import tweepy
import os, sys
import json
import pytz
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
from http.client import IncompleteRead
import threading, logging, time
from kafka import KafkaConsumer, KafkaProducer
from tzlocal import get_localzone


consumer_key = "yourKey"
consumer_secret = "yourSecret"
access_token = "yourToken"
access_token_secret = "yourTokenSecret"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


languages_option = ['en']
track_keywords = ['Taiwan economic', 'OBOR','Tsai Ing-Wen', 'cross-Strait', 'Taiwan business',
                  'taipei economic', '@Taiwan_Today','economic woes of taiwan', 'Lin Chuan', 'AB106']
follow_users_ids = ['759251', '20402945', '2768501', '15012486', '1367531', '34713362']
# @cnn => 759251
# @cnbc => 20402945
# @abcnews => 2768501
# @cbsnews => 15012486
# @foxnews => 1367531
# @business => 34713362


class TweetStreamListener(StreamListener):
    def on_data(self, data):
        #if  not decoded[`text`].startswith('RT'):
        try:
            dict_data = json.loads(data)
            author = dict_data["user"]["screen_name"]
            message = dict_data["text"]
            # timeStamp = datetime.strptime(dict_data["created_at"].replace("+0000 ", ""), "%a %b %d %H:%M:%S %Y").isoformat(' ')

            # postTime is datetime object
            postTime = datetime.strptime(dict_data["created_at"].replace("+0000 ", ""), "%a %b %d %H:%M:%S %Y")
            # postTime2 is datetime object => timestamp object
            postTime2 = time.mktime(postTime.timetuple())
            # postTime3 is timestamp object => datetime object
            postTime3 = time.localtime(postTime2 + 28800)
            # localTime is datetime object => string object
            localTime = time.strftime("%Y-%m-%d %H:%M:%S", postTime3)

            words = message.split(" ")
            if (words[0] == "RT"):
                pass
            else:
                print(author)
                print(localTime)
                print(message)

                twitterMessage = ("Author:" + author + '\n' + "PostTime:" + localTime + '\n' + "Context:" + message)
                # transformation from string => byte
                byte_twitterMessage = str.encode(twitterMessage)

                producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(0,9))
                producer.send('twitterStreaming', byte_twitterMessage)
                producer.flush()

        except:
            print ("processing exception")

        return True

    # on failure
    def on_error(self, status):
        print (status)

if __name__ == "__main__":
    listener = TweetStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        try:
            stream = Stream(auth, listener)
            stream.filter(track= track_keywords, languages = languages_option,)
            # stream.filter(track = ['Jennifer Lopez'], languages = ['en'])
            # stream.filter(follow = follow_users_ids, languages = ['en'])
        except IncompleteRead:
            pass
        except KeyboardInterrupt:
            stream.disconnect()
            break
