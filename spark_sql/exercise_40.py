"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-First-Non-Null-Value-per-Group.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import first

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_40')
         .getOrCreate())

df = spark.createDataFrame([
    (None, 0),
    (None, 1),
    (2, 0),
    (None, 1),
    (4, 1)
],["id","group"])

df.groupby("group").agg(first("id", ignorenulls=True)).orderBy("group").show()