from pyspark.sql import SparkSession, HiveContext
from pyspark import SparkContext

spark = (SparkSession
         .builder
         .appName('exersice_12')
         .getOrCreate())

df = spark.read.options(delimiter='|',comment='+',header=True).csv('input.csv')

df = df.drop('_c0')
df = df.drop('_c4')
df.show()