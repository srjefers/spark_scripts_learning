"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-pivot-to-generate-a-single-row-matrix.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_27')
         .getOrCreate())

df = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')
#df.show()
df = df.groupBy().pivot("udate",['udate','20090622','20090624','20090626','20090629','20090914']).sum("cc")
df = df.withColumn("udate",lit('cc'))
df.show()