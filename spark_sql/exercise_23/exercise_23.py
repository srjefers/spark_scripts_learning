"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-value-agg.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col, regexp_replace, try_to_number, lit
from pyspark.sql.types import IntegerType

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_23')
         .getOrCreate())

df = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')

df = df.withColumn('population', regexp_replace(df.population," ","").cast(IntegerType()))

df.select(max(col('population'))).show()
df.printSchema()
#df.show()