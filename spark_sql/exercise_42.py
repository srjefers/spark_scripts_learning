"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-Most-Common-Non-null-Prefix-Occurences-per-Group.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_42')
         .getOrCreate())

