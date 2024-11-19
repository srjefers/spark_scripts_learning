"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/How-to-add-days-as-values-of-a-column-to-date.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, col, to_date
from pyspark.sql.types import IntegerType

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_21')
         .getOrCreate())

df = spark.createDataFrame([
    (0, "2016-01-1"),
    (1, "2016-02-2"),
    (2, "2016-03-22"),
    (3, "2016-04-25"),
    (4, "2016-05-21"),
    (5, "2016-06-1"),
    (6, "2016-03-21")
],["number_of_days", "date"])


df = df.withColumn('future', date_add(col('date'), df.number_of_days.cast(IntegerType())))
df.printSchema()
df.show()