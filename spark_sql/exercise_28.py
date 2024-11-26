"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Using-pivot-for-Cost-Average-and-Collecting-Values.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import first

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_28')
         .getOrCreate())

df = spark.createDataFrame([
    (0, "A", 223, "201603", "PORT"),
    (0, "A", 22, "201602", "PORT"),
    (0, "A", 422, "201601", "DOCK"),
    (1, "B", 3213, "201602", "DOCK"),
    (1, "B", 3213, "201601", "PORT"),
    (2, "C", 2321, "201601", "DOCK")
],["id","type", "cost", "date", "ship"])

result_1 = df.groupBy('id','type').pivot('date',['201603','201602','201601']).agg(first('cost'))
result_1.show()

result_2 = df.groupBy('id','type').pivot('date',['201603','201602','201601']).agg(first('ship'))
result_2.show()
df.show()