"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-pivot-to-generate-a-single-row-matrix.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_27')
         .getOrCreate())

df = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')

df.show()
