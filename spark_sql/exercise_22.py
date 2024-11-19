"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-UDFs.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_22')
         .getOrCreate())

df = spark.createDataFrame([
    ("a"),
    ("b"),
    ("c"),
    ("d"),
    ("d")
],StringType()).toDF("letter")

def toUpperCase(text: str) -> str:
    return text.upper()

toUpperCaseUdf = udf(toUpperCase, StringType())

df = df.withColumn("letterUpperCase", toUpperCaseUdf(col("letter")))

df.show()