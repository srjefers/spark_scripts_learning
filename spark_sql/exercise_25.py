"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-values-per-group-groupBy.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_25')
         .getOrCreate())

nums = spark.range(5).withColumn("group", col("id") % 2)

nums.groupBy(['group']).agg(collect_list('id')).show()
nums.show()