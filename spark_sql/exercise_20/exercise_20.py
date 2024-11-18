"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Why-are-all-fields-null-when-querying-with-schema.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_20')
         .getOrCreate())

df = spark.read.options(delimiter='|',inferSchema=True).csv('data.csv')
df = df.withColumn('dateTime', to_timestamp(col("_c0"),"yyyy-MM-dd HH:mm:ss,SSS"))
df = df.withColumn('IP', col('_c1'))

df = df.drop('_c0')
df = df.drop('_c1')
df = df.drop('_c2')
df.printSchema()
df.show()