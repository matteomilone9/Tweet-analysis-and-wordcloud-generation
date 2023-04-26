###LIBRERIE
import tweepy
from wordcloud import WordCloud
import matplotlib.pyplot as plt
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
import nltk
from nltk import *
from nltk.corpus import *
from nltk.stem import *
from nltk.stem.porter import *
download("stopwords")
stopwords = stopwords.words('english')
download("punkt")  # download data needed by word_tokenizer
import time

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

###CREAZIONE FUNZIONE DI PREPROCESSING E CONVERSIONE DELLA FUNZIONE IN UDF
def preprocess(x):
    #converti in minuscolo
    x = x.lower()
    #tokenize
    tokens = nltk.word_tokenize(x)
    tokens_x =  [w for w in tokens if w.isalpha()]   
    #stopwords
    stop_x = [word for word in tokens_x if not word in stopwords]
    #remove "rt"
    no_rt_x = [word for word in stop_x if not re.search(r'\brt\b', word) 
               and not re.search(r'http\S+', word)]
    #stemming
    stemmer = PorterStemmer()
    stemm_x = [stemmer.stem(word) for word in no_rt_x]
    #restore sentences
    return ( " ".join(stemm_x))
        
###CONV
preprocess_udf = udf(preprocess)

###LETTURA DATI DA MONGO USANDO SPARK
dfSpark = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

###UTILIZZO PREPROCESS
dfSpark = dfSpark.withColumn('text', preprocess_udf(dfSpark['text']))
text_col = dfSpark.select('text')

###SALVATAGGIO IN COLLECTION2 DOPO PREPROCESS
dfSpark.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

###CREAZIONE WORDCLOUD
#splittaggio delle parole in "text"
words = dfSpark.select(explode(split("text", "\s+")).alias("word"))

#aggregazione parole per frequenza
word_freq = words.groupBy("word").count()

#conversione dfSpark in dict()
word_dict = dict(word_freq.collect())

#creazione e visualizzazione del grafico
wordcloud = WordCloud(width=800, height=800, background_color='white').generate_from_frequencies(word_dict)
plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)
plt.show()
            
#
print("FINE CICLO")