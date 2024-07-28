from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import first, col,explode

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
df = df.withColumn('abc', explode(col('values')))

df.show()