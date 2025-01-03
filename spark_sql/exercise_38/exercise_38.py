"""
    ulr: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Working-with-Datasets-Using-JDBC-and-PostgreSQL.html
    jdbc: https://jdbc.postgresql.org/download/
"""
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName('exercise_38')
         .config('spark.jars','./postgresql-42.7.4.jar')
         .getOrCreate())

df = spark.read \
        .format('jdbc') \
        .option('url','jdbc:postgresql://localhost:5432/postgres') \
        .option('dbtable', 'tmp_data') \
        .option('user','postgres') \
        .option('password', 'root') \
        .option('driver', 'org.postgresql.Driver') \
        .load()

df.printSchema()