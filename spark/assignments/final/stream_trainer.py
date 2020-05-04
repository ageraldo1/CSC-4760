from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import functions as app

# Spark Context
conf = SparkConf().setMaster('local[2]').setAppName('ApacheWebLogsStream')
sc = SparkContext(conf=conf)
sc.setLogLevel('OFF')

# Spark Streaming Context
ssc = StreamingContext(sparkContext=sc, batchDuration=10) 
input_stream = ssc.socketTextStream(hostname='localhost', port=9999)

# Process stream
input_stream.foreachRDD(app.process)

ssc.start()
ssc.awaitTerminationOrTimeout(timeout=900)
ssc.stop()
