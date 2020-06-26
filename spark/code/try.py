from __future__ import print_function
import json
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from pyspark.sql import types as tp
from elasticsearch import Elasticsearch
import pyspark.sql.functions as f


import pandas as pd
import numpy as np

# Text cleaning
import nltk

# ML Libraries
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

nltk.download('stopwords')

STOP_WORDS = nltk.corpus.stopwords.words('english')

brokers="10.0.100.23:9092"
topic = "reddit-comments"
elastic_host = "10.0.100.51"
elastic_index = "reddit"
elastic_document = "_doc"

es_conf = {
    "es.nodes" : elastic_host,
    "es.port" : '9200',
    "es.resource" : '%s/%s' % (elastic_index,elastic_document),
    "es.input.json" : "yes",
    "es.batch.size.entries": "1"
}

redditPreSchema = tp.StructType([
    # Todo use proper timestamp
    tp.StructField(name= 'created_utc', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'subreddit',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'author',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'body',      dataType= tp.StringType(),  nullable= True)
])

predictionSchema = tp.StructType([
    # Todo use proper timestamp
    tp.StructField(name= 'created_utc', dataType= tp.TimestampType(),  nullable= True),
    tp.StructField(name= 'subreddit',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'author',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'body',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'prediction',      dataType= tp.FloatType(),  nullable= True),
    tp.StructField(name= 'words',      dataType= tp.StringType(),  nullable= True)
])

sc = SparkContext(appName="RedditStreaming")
spark = SparkSession(sc)

sc.setLogLevel("WARN")

# Elastic Search
conf = SparkConf(loadDefaults=False)
conf.set("es.index.auto.create", "true")

# We have two different csv because we had a problem saving the dataframe to csv  
comments = pd.read_csv('../tap/spark/dataset/reddit_comments.csv')
score = pd.read_csv('../tap/spark/dataset/reddit_score.csv')

# Join the two columns in a single dataframe
training_set = pd.DataFrame(columns=['text', 'score'])
training_set['text'] = comments['text']
training_set['score'] = score['score']
training_set = training_set.dropna()

# Steps for our pipeline
lr_pipeline = Pipeline([('vect', CountVectorizer()),
                        ('tfidf', TfidfTransformer()),
                        ('model', LogisticRegression()),
                        ])


# Fit the pipeline model with the training data
print("FITTING")
lr_pipeline.fit(training_set['text'], training_set['score'])
print("FITTED")

def elaborate(key, rdd):
  print("********************")
  comment = rdd.map(lambda value: json.loads(value[1])).map(
    lambda json_object: (
      json_object["created_utc"], 
      json_object["subreddit"], 
      json_object["author"], 
      json_object["body"]
    )
  ) 

  rString = comment.collect()
  if not rString:
    print("No comments")
    return
  
  print("********************")

  # Starting RDD with the data from kafka 
  rowRdd = comment.map(lambda t: Row(created_utc=t[0], subreddit=t[1], author=t[2], body=t[3], score=t[4]))

  # Converting the RDD into a spark dataframe
  wordsDataFrame = spark.createDataFrame(rowRdd, schema=redditPreSchema)

  # Converting the UTC to timestamp
  wordsDataFrame = wordsDataFrame.toPandas()
  wordsDataFrame['created_utc'] = pd.to_datetime(wordsDataFrame['created_utc'], dayfirst=True, unit='s')
  wordsDataFrame['created_utc'] = wordsDataFrame['created_utc'].dt.tz_localize('UTC').dt.tz_convert('Europe/Rome')
  print("DATE")
  print(wordsDataFrame['created_utc'])

  # Making predictions
  feature = wordsDataFrame[wordsDataFrame['body'].notna()]
  data = lr_pipeline.predict(feature['body'])
  feature['prediction'] = data

  # Removing stop words for visualization(maybe going to remove)
  feature['words'] = feature['body'].apply(lambda x: ' '.join(word for word in x.split() if word not in STOP_WORDS))

  # Converting the pandas dataframe into a spark dataframe so that we can stream
  wordsDataFrame = spark.createDataFrame(feature, schema=predictionSchema)

  new = wordsDataFrame.rdd.map(lambda item: {'created_utc' : item['created_utc'], 'subreddit' : item['subreddit'], 'author' : item['author'], 'body' : item['body'], 'prediction' : item['prediction'], 'words' : item['words']})
  finalRdd = new.map(lambda x: json.dumps(x, default=str)).map(lambda x: ('key', x))
  wordsDataFrame.show()

  finalRdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_conf)

ssc = StreamingContext(sc, 3)

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list" : brokers})
print("FOR EACH RDD")
kvs.foreachRDD(elaborate)
print("END FOR EACH RDD")


mapping = {
  "settings": {
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_" 
        },
        "english_keywords": {
          "type":       "keyword_marker",
          "keywords":   ["example"] 
        },
        "english_stemmer": {
          "type":       "stemmer",
          "language":   "english"
        },
        "english_possessive_stemmer": {
          "type":       "stemmer",
          "language":   "possessive_english"
        }
      },
      "analyzer": {
        "rebuilt_english": {
          "tokenizer":  "standard",
          "filter": [
            "english_possessive_stemmer",
            "lowercase",
            "english_stop",
            "english_keywords",
            "english_stemmer"
          ]
        }
      }
    }
  },
    "mappings": {
        "properties": {
            "created_utc": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ssZZ||yyyy-MM-dd HH:mm:ss"
            },
            "body": {
                "type": "text",
                "fielddata": True
            },
            "author": {
                "type": "text",
                "fielddata": True
            },
            "subreddit": {
                "type": "text",
                "fielddata": True
            },
            "prediction": {
                "type": "float"
            },
            "words": {
                "type": "text",
                "fielddata": True
            }
        }
    }
}

elastic = Elasticsearch(hosts=[elastic_host])

response = elastic.indices.create(
    index = elastic_index,
    body = mapping,
    ignore = 400
)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])

elif 'error' in response:
        print ("ERROR:", response['error']['root_cause'])
        print ("TYPE:", response['error']['type'])

ssc.start()
ssc.awaitTermination()