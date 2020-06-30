# Reddit-Analyzer

## Goal

The end product you will get building this project is an insight analysis on reddit posts/comments.
You will be building an end-to-end pipeline, starting from data ingestion to visualization. 

## Why do we need that?

A huge amount of data flows through social networks. 
Huge amount of data translates in an endless potential to build something useful.

## Requirements
In order to accomplish our goal we need a few things:
* **Data from reddit** 
* **A way to stream those data**
* **A way to manipulate data in streaming**
* **A way to FAST analyze our data**
* **A way to provide those analytics to a end-user**

## Data Pipeline

![Pipeline]()

## Step 1

Start zookeeper and kafka server. <br/>This allows to reliably stream big amount of data fast and easy.

Move to ```/kafka``` and type  

```docker-compose up``` 

on your terminal.

This wil bring up both zookeeper and kafka server.

## Step 2

Start reddit connector. This will get streaming data from posts and comments in the desidered subreddit (which you can set on reddit.env file).

Move to ```/bin``` and type

```./startRedditConnector.sh```

This will start a docker container running the kafka connector which will stream data in two different kafka topics:
* **reddit-posts**
* **reddit-comments**

## Step 3 

Start Elastic Search and Kibana which will help later on for Data Indexing and visualization. <br/>
Elastic Search is used to fast aggregate streaming data, thanks to its lighting speed.

Move to ```/elasticsearch``` and type

```docker-compose up```

## Step 4

Start Spark Streaming in order to perform Data Elaboration, cleaning and predictions.

### Elaboration steps

* **Fit our model with our training data**
* **Make a spark rdd containing just the intresting field from kafka streamed data**
* **Convert the unix epoch time to timestamp**
* **Apply some NLP**
* **Predict using the trained model**
* **Dump to a JSON**
* **Send to Elastic Search**

Move to ```/bin``` and type

```./sparkSubmitPython.sh comment_stream.py "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.6,org.elasticsearch:elasticsearch-hadoop:7.7.0"``` <br/>

## Step 5

If your machine is still responsive and fully working after this, it's time to pratically see what we've achieved so far. <br/>
Go to [Kibana](http://localhost:5061) in order to visualize the streaming data and what is trending!
