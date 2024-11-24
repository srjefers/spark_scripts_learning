"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Multiple-Aggregations.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_26')
         .getOrCreate())

nums = spark.range(5).withColumn("group", col("id") % 2)

nums.groupBy(['group']).agg(max('id').alias('max_id'), min('id').alias('min_id')).show()