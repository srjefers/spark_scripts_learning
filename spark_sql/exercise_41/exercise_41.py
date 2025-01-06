"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-Longest-Sequence.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, col, count

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_41')
         .getOrCreate())

df = spark.read.options(delimiter=',', inferSchema=True, header=True).csv('data.csv')

w = Window.partitionBy('id').orderBy('time')
result = df.select(col('id'), col('time'), rank().over(w).alias('rnk')).\
            groupBy(col('id'),col('time'),col('rnk')).\
            agg(count('*').alias('count'))
#df = df.withColumn('drank', rank().over(w))
#df.show()

result.show()
