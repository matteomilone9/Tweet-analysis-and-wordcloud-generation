###LIBRERIE
import tweepy
#from wordcloud import WordCloud
#import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pymongo
from pymongo import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import *
from pyspark.ml import *
import warnings
warnings.filterwarnings("ignore") #nasconde i messaggi di avviso 
#e non quelli di errore
import re
import configparser
from collections import *
import requests
import base64
#import nltk
#from nltk import *
#from nltk.corpus import *
#from nltk.stem import *
#from nltk.stem.porter import *
#download("stopwords")
#stopwords = stopwords.words('english')
#download("punkt")  # download data needed by word_tokenizer
import time

###LETTURA CREDENZIALI --- INSERIRE I PROPRI TKNS
api_key = 
api_key_secret = 
access_token = 
access_token_secret = 
bearer_token = 


###AUTENTIFICAZIONE
client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)
auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
api = tweepy.API(auth)

#CONFIGURAZIONE CLIENT MONGODB
client = pymongo.MongoClient(#INSERIRE PORTA DI MONGODB)
db = client[#INSERIRE NOME DATABASE MONGO]
collection1 = db[#INSERIRE NOME COLLEZIONE 1 ]
collection2 = db[#INSERIRE NOME COLLEZIONE 2 ]

###CREAZIONE SESSIONE SPARK E CONNETTORE MONGO
spark = SparkSession\
    .builder \
    .appName("TweetAnalysis") \
    .config("spark.mongodb.input.uri", #CLIENT/DB.COLLECTION1) \
    .config("spark.mongodb.output.uri", #CLIENT/DB.COLLECTION2) \
    .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:2.4.2")\
    .getOrCreate()

####CREAZIONE BOT PER LA RICERCA 
class MyStream(tweepy.StreamingClient):
    MAX_TWEETS = 10 #limite
    tweet_count = 0 

    #avvisa dall'avvio
    def on_connect(self):
        print("CONNESSO")

    #collezione dei tweet fino al raggiungimento del limite di tweet nella sessione
    def on_tweet(self, tweet):
        if self.tweet_count >= self.MAX_TWEETS:
            self.disconnect()
            return
        
        # Controlla se il numero di tweet raccolti ha raggiunto la soglia massima   
        if tweet.referenced_tweets == None: #nn prende i retweet
            #print(tweet.text)
            tweet_data = {
                "text": tweet.text,
                "created_at": tweet.created_at}
            collection1.insert_one(tweet_data)
            self.tweet_count += 1
        
###CREAZIONE DELLO STREAM/CLIENT
stream = MyStream(bearer_token=bearer_token)

###PAROLE DA INSERIRE COME RULES
keywords = [#INSERIRE TERMS]

###AVVIO STREAM
#per gestire le rules di ricerca o attivare dry_run=True
#o usare stream.get_rules() e stream.delete_rules(id)
for term in keywords:
    stream.add_rules(tweepy.StreamRule(term), dry_run=True) #LE REGOLE SI AGGIORNANO

stream.filter(tweet_fields=["referenced_tweets"])

###MESSAGGIO DI END
print("STREAMING ENDED! ")

