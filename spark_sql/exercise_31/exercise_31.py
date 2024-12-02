"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Flattening-Dataset-from-Long-to-Wide-Format.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, first,lit, col

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_31')
         .getOrCreate())

df = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')

df = df.groupBy('key').pivot('date').agg(first('val1').alias('v1'),first('val2').alias('v2'))

df.show()