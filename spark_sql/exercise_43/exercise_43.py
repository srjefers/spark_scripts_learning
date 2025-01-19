"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Developing-Custom-Data-Source.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_43')
         .getOrCreate())