"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Using-pivot-for-Cost-Average-and-Collecting-Values.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, first,lit, col

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_29')
         .getOrCreate())

df = spark.createDataFrame([
  (100,1,23,10),
  (100,2,45,11),
  (100,3,67,12),
  (100,4,78,13),
  (101,1,23,10),
  (101,2,45,13),
  (101,3,67,14),
  (101,4,78,15),
  (102,1,23,10),
  (102,2,45,11),
  (102,3,67,16),
  (102,4,78,18)
],["id","day", "price", "units"])

result = df.groupBy('id').pivot('day',['1','2','3','4']).agg(first('price').alias('price'), first('units').alias('units'))

#result.show()
result.select([col(c).name('_'.join(x for x in c.split('_')[::-1])) for c in result.columns]).show()