"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-rollup-Operator-for-Total-and-Average-Salaries-by-Department-and-Company-Wide.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_44')
         .getOrCreate())

df = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')

df.rollup(df.department).agg(sum('salary').alias('sum'), avg('salary').alias('avg')).show()