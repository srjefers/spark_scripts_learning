"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-explode-Standard-Function.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,ArrayType, IntegerType
from pyspark.sql.functions import col, explode

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_17')
         .getOrCreate())
"""
data = [1,2,3]
rdd = sc.parallelize(data)

df = spark.createDataFrame(rdd.flatMap(lambda x: [[data, x]]).collect(),['nums','num'])
"""

df = spark.createDataFrame([[[1,2,3]]])
df = df.select(col("_1").alias('nums'), explode(col("_1")).alias('num'))
df.show()