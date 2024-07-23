from pyspark.sql import SparkSession, HiveContext
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_11')
         .getOrCreate())

data = [1,2,3]
rdd = sc.parallelize(data)

df = spark.createDataFrame(rdd.flatMap(lambda x: [[data, x]]).collect(),['nums','num'])
df.show()