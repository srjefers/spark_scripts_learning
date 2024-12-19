"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Calculating-percent-rank.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import percent_rank, when, col

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_37')
         .getOrCreate())

df = spark.read.options(delimiter=',', inferSchema=True, header=True).csv('data.csv')

w = Window.orderBy("Salary")
df = df.withColumn('Percentage', percent_rank().over(w))\
        .withColumn('Percentage', when(col('Percentage') > 0.61, 'High')\
                                    .when(col('Percentage') <= 0.5, 'Low')\
                                    .otherwise('Average'))

df.orderBy('Salary').show()