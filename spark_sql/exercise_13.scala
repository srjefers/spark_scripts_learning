"""
  This code has been taked and manipulated from an answer from 
  https://stackoverflow.com/a/74775611/7102575
  I am going to break the code to understand how it works and translate it into
  pyspark. 

  It 100% worked on scala.
"""
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType}
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val spark = SparkSession.builder.getOrCreate()


val df = Seq(
  Seq("a","b","c"),
  Seq("X","Y","Z")).toDF


df.show(false)

df.withColumn("id2", monotonically_increasing_id())
  .select(col("id2"), posexplode(col("value")))
  .withColumn("id", col("pos") + 1)
  .groupBy("id2").pivot("id").agg(first("col")).drop("id2")
  .show(false)







"""from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import first, col,explode, size, max

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
df = df.drop('_1')
df = df.withColumn('_length', size(col('values')))

df_max_value = df.select(max(col('_length'))).collect()[0]['max(_length)']

df = 

df.show()

"""