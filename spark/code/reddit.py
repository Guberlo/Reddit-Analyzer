from pyspark.sql import SparkSession
from pyspark.sql import functions as f
data = "tap/spark/dataset/Sentiment.csv"
spark = SparkSession.builder.appName("Reddit").getOrCreate()
data_set = spark.read.csv(data, header=True)
data_set = data_set.withColumn("Sentiment", data_set["Sentiment"].cast("double"))


positiveSentiment = data_set.filter(data_set.Sentiment > 0.0).count()
negativeSentiment = data_set.filter(data_set.Sentiment < 0).count()

print("Number of positive reviews: {} \nNumber of negative reviews: {}".format(positiveSentiment, negativeSentiment))

df = data_set.filter(data_set.Comment.contains("beautiful"))

df.show()

numOfSubmissions = data_set.groupBy('User').count()
numOfSubmissions.show()

maxUsers = numOfSubmissions.filter(numOfSubmissions['count'] > 3)
maxUsers.show()

totSentimentIWW4 = data_set.filter(data_set.User == "IWW4").rdd.map(lambda x: float(x["Sentiment"])).reduce(lambda x, y: x+y)
print("Sum of IWW4 sentiment : {}".format(totSentimentIWW4))

#meanSentimentIWW4 = data_set.filter(data_set.User == "IWW4").select(avg('Sentiment')).show()
meanSentimentIWW4 = data_set.filter(data_set.User == "IWW4").rdd.map(lambda x: float(x["Sentiment"])).reduce(lambda x, y: x+y) / 4
print("Mean of IWW4 sentiment : {}".format(meanSentimentIWW4))

