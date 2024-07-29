"""
  This code has been taked and manipulated from an answer from 
  https://stackoverflow.com/a/74775611/7102575
  I am going to break the code to understand how it works and translate it into
  pyspark. 

  It 100% worked on scala.
  https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.posexplode.html
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col, posexplode, monotonically_increasing_id

spark = (SparkSession
         .builder
         .appName('exersice_13')
         .getOrCreate())

data = [
    [['a','b','c']],
    [['X','Y','Z']]
]

df = spark.createDataFrame(data)
df = df.withColumn('values', col('_1'))

df.withColumn('id_c', monotonically_increasing_id()).select(col('id_c'), posexplode(df.values)).groupBy('id_c').pivot('pos').agg(first('col')).drop('id_c').show()
