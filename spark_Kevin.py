import os
from datetime import datetime

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from geopy.geocoders import Nominatim
from nltk import tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from unique_id import get_unique_id

# Authentication Variables for ElasticSearch
load_dotenv()
CLOUD_ID = os.environ.get("CLOUD_ID")
USER_AUTH = os.environ.get("CLOUD_IDUSER_AUTH")
USER_PASS = os.environ.get("USER_PASS")

TCP_IP = 'localhost'
TCP_PORT = 9001


def processTweet(tweet):
    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        rawLocation = tweetData[0]
        hashtag = tweetData[1]
        text = tweetData[2]
        print()

        ###################################
        #### Sentiment Analysis Section ###
        ###################################
        sid = SentimentIntensityAnalyzer()
        ss = sid.polarity_scores(text)
        compound = ss[sorted(ss)[0]]
        polarity = "neutral"
        if compound >= .05:
          polarity = "positive"
        elif compound <= -.05:
          polarity = "negative"
      
      
        ############################
        #### Geolocation Section ###
        ############################
        lat, lng = None, None
        result = None
        geolocator = None
        #try:
        geolocator = Nominatim(user_agent="TwitterSentinemtAnalysisBigData")
        result = geolocator.geocode(rawLocation)
        #except:
        print("Error trying to geolocate the address: ", rawLocation)
          #return

        try:
          if result:
            lat = result.latitude
            lng = result.longitude
        except:
          print("Error trying to parse location the result:\n", result, "\n")
          return;

        location = None
        country = None
        if lat and lng:
          location = (lat, lng)

          try:
            reversedLocation = geolocator.reverse(str(lat) + ", " + str(lng))
            if reversedLocation:
              country = reversedLocation.raw["address"]["country"]
          except:
            print("Error trying to get country reversed")
            return
          
          
        #######################
        #### Result Section ###
        #######################
        tweet = { "hashtag": hashtag, "text": text, "text_terms": text.split(), "rawLocation": rawLocation, 
                 "polarity": polarity, "compound": compound, 
                 "location": {"lat": lat, "lon": lng},
                 "country": country, "timestamp": datetime.now()}
        print("\n\n")
        for key, value in tweet.items():
           print("{}: {}".format(key, value))

        # (iii) Post the index on ElasticSearch or log your data in some other way (you are always free!!) 
        
        es = Elasticsearch(cloud_id=CLOUD_ID, http_auth=(USER_AUTH, USER_PASS))
        newId = get_unique_id()
        res = es.index(index='hashtags', id=newId, body=tweet)
               
        
# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext.getOrCreate()

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
#ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))

ssc.start()
ssc.awaitTermination()
