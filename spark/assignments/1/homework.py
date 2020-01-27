from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName('PySpark Homework 1').setMaster('local[2]')
sc = SparkContext(conf=conf)

lines = sc.textFile('/tmp/walking/dataset.csv')
print(lines.count())
