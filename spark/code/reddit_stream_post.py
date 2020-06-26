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
import re 
import sys
import config



brokers="10.0.100.23:9092"
topic = "reddit-posts"
elastic_host = "10.0.100.51"
elastic_index = "redditpost"
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
    tp.StructField(name= 'title',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'url',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'subreddit',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'id',      dataType= tp.StringType(),  nullable= True)
])

redditPostSchema = tp.StructType([
    # Todo use proper timestamp
    tp.StructField(name= 'created_utc', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'title',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'url',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'subreddit',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'id',      dataType= tp.StringType(),  nullable= True)
])


sc = SparkContext(appName="RedditStreaming")
spark = SparkSession(sc)

sc.setLogLevel("WARN")

# Elastic Search
conf = SparkConf(loadDefaults=False)
conf.set("es.index.auto.create", "true")

def elaborate(key, rdd):
  print("********************")
  comment = rdd.map(lambda value: json.loads(value[1])).map(
    lambda json_object: (
      json_object["created_utc"], 
      json_object["title"], 
      json_object["url"], 
      json_object["subreddit"],
      json_object["id"],
    )
  ) 

  rString = comment.collect()
  if not rString:
    print("No posts")
    return
  
  print("********************")

  # INSERT HERE SOMEWHERE THE CALL TO GET_FLAIR
  
  rowRdd = comment.map(lambda t: Row(created_utc=t[0], title=t[1], url=t[2], subreddit=t[3], id=t[4]))
  #rowRdd = comment.map(lambda t: Row(subreddit=t[0], author=t[1], body=t[2]))
  wordsDataFrame = spark.createDataFrame(rowRdd, schema=redditPreSchema)
  wordsDataFrame = wordsDataFrame.withColumn('created_utc', f.date_format(wordsDataFrame.created_utc.cast(dataType=tp.TimestampType()), "yyyy-MM-dd HH:mm:ss"))
  pruk = wordsDataFrame.select('created_utc')
  pruk.show()
  #wordsDataFrame = wordsDataFrame.withColumn('created_utc', f.to_timestamp('created_utc', "yyyy-MM-dd HH:mm:ss"))
  wordsDataFrame = spark.createDataFrame(wordsDataFrame.collect(), schema=redditPostSchema)
  print('*****************************')
  print('*****************************')
  new = wordsDataFrame.rdd.map(lambda item: {'created_utc' : item['created_utc'], 'title' : item['title'], 'url' : item['url'], 'subreddit' : item['subreddit'], 'id' : item['id']})
  #new = wordsDataFrame.rdd.map(lambda item: {'subreddit' : item['subreddit'], 'author' : item['author'], 'body' : item['body']})
  finalRdd = new.map(lambda x: json.dumps(x, default=str)).map(lambda x: ('key', x))
  #print("FINAL RDD")
  #wordsDataFrame.show()
  #print(finalRdd.collect())
  #print("RDD PRINTED: ")

  finalRdd.saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_conf)

ssc = StreamingContext(sc, 5)

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
            "title": {
                "type": "text",
                "fielddata": True
            },
            "url": {
                "type": "text",
            },
            "subreddit": {
                "type": "text",
                "fielddata": True
            },
            "id" : {
              "type": "text"
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