"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/explode-structs-array.html
"""

from pyspark.sql.functions import col, explode
from pyspark.sql import SparkSession, HiveContext

spark = (SparkSession
         .builder
         .appName('exersice_7')
         .getOrCreate())

df = spark.read.json('input.json', multiLine=True)

df = df.withColumn('Monday', df.hours.getItem('Monday'))
df = df.withColumn('Thursday', df.hours.getItem('Thursday'))
df = df.withColumn('Wednesday', df.hours.getItem('Wednesday'))
df = df.withColumn('Tuesday', df.hours.getItem('Tuesday'))
df = df.withColumn('Friday', df.hours.getItem('Friday'))
df = df.withColumn('Saturday', df.hours.getItem('Saturday'))
df = df.withColumn('Sunday', df.hours.getItem('Sunday'))

df = df.drop(col('hours'))

df = df.melt(["business_id","full_address"],['Monday','Thursday','Wednesday','Thursday','Friday','Saturday','Sunday'],'day','time')
df = df.withColumn('open_time', df.time.getItem('open'))
df = df.withColumn('close_time', df.time.getItem('close'))
df = df.drop('time')
df.show()