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

brokers="192.168.1.2:9092"
topic = "reddit-comments"
elastic_host = "192.168.1.2"
elastic_index = "reddit"
elastic_document = "_doc"

es_conf = {
    "es.nodes" : elastic_host,
    "es.port" : 9200,
    "es.resource" : '%s/%s' % (elastic_index,elastic_document),
    "es.input.json" : "yes"
}

redditSchema = tp.StructType([
    # Todo use proper timestamp
    tp.StructField(name= 'created_utc', dataType= tp.LongType(),  nullable= True),
    tp.StructField(name= 'subreddit',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'author',      dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'body',      dataType= tp.StringType(),  nullable= True)
])

sc = SparkContext(appName="RedditStreaming")
spark = SparkSession(sc)

sc.setLogLevel("WARN")

# Elastic Search
conf = SparkConf(loadDefaults=False)
conf.set("es.index.auto.create", "true")

def elaborate(key, rdd):
  print("********************")
  comment = rdd.map(lambda (key, value): json.loads(value).map(
    lambda json_object: (
      json_object["created_utc"], 
      json_object["subreddit"], 
      json_object["author"], 
      json_object["body"]
    )
  ) 
)

  rString = comment.collect()
  if not rString:
    print("No comments")
    return
  
  print("********************")
  print(rString)

  rowRdd = comment.map(lambda t: Row(created_utc=t[0], subreddit=t[1], author=t[2], body=t[3]))
  wordsDataFrame = spark.createDataFrame(rowRdd, redditSchema)
  wordsDataFrame.show()
  new = wordsDataFrame.rdd.map(lambda item: {'created_utc' : item['created_utc'], 'subreddit' : item['subreddit'], 'author' : item['author'], 'body' : item['body']})
  finalRdd = new.map(json.dumps).map(lambda x: ('key', x))
  print("FINAL RDD: ")
  print(finalRdd.collect())
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
                "type": "date"
            },
            "body": {
                "type": "text",
                "fielddata": True
            },
            "author": {
                "type": "text",
            }
            ,
            "subreddit": {
                "type": "text",
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