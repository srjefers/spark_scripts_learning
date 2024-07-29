"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Most-Populated-Cities-Per-Country.html
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import regexp_replace, col, row_number, desc
from pyspark.sql.types import IntegerType
import re

spark = (SparkSession
         .builder
         .appName('exersice_15')
         .getOrCreate())

df = spark.read.options(delimiter='|',comment='+',header=True).csv('input.csv')

for each in df.schema.names:
    df = df.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '')))

df = df.drop('_c0').drop('_c4')
#df.show()

w = Window.partitionBy('country').orderBy(col('population_int').desc())

df = df.withColumn('population_clean',regexp_replace(col('population'),' ','')) \
    .withColumn('population_int',col('population_clean').cast(IntegerType())) \
    .drop('population_clean') \
    .drop('population') \
    .withColumn('rn', row_number().over(w)) \
    .filter(col('rn') == 1) \
    .select(col('name'),col('country'), col('population_int').alias('population'))

df.show()
