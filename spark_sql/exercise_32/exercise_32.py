"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-1st-and-2nd-Bestsellers-Per-Genre.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_32')
         .getOrCreate())

df = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')

windowFunc = Window.partitionBy('genre').orderBy(desc('quantity'))
df = df.withColumn("rnk", rank().over(windowFunc))
df.filter('rnk in (1,2)').show()