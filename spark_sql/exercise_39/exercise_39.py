"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Specifying-Table-and-SQL-Query-on-Command-Line.html
"""
from pyspark.sql import SparkSession
import os
import argparse

def main(params):
    spark = (SparkSession
            .builder
            .appName('exercise_38')
            .config('spark.jars','./postgresql-42.7.4.jar')
            .getOrCreate())

    df = spark.read \
            .format('jdbc') \
            .option('url','jdbc:postgresql://localhost:5432/postgres') \
            .option('dbtable', str(params.table_name)) \
            .option('user','postgres') \
            .option('password', 'root') \
            .option('driver', 'org.postgresql.Driver') \
            .load()

    df.createOrReplaceTempView('tmp_data_df')
    df = spark.sql(str(params.sql_query))
    
    df.show()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--table_name')
    parser.add_argument('--sql_query')

    args = parser.parse_args()
    main(args)