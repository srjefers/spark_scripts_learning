"""
    url: https://nxcals-docs.web.cern.ch/current/user-guide/examples/standalone-app/
    execution: /opt/spark/bin/spark-submit /home/kernelpanic/Documents/SPARK/spark_sql/exercise_16/exercise_16.py --csv_file=/home/kernelpanic/Documents/SPARK/spark_sql/exercise_16/input.csv --columns=city,country
"""
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import upper, col
import re
import argparse
import os


def main(args: dict) -> DataFrame:
    csv_file = args.csv_file
    column_cast = args.columns
    print('------------------------------')
    print(column_cast)
    # ['city']

    conf = (SparkConf()
            .setAppName("exersice_16")
            .setMaster('local')
            .set('spark.driver.memory', '4G')
            )
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    df = spark.read.options(delimiter='|',comment='+',header=True).csv(csv_file)
    df = df.drop(df.schema.names[0]).drop(df.schema.names[-1])
    df.show()

    for each in df.schema.names:
        df = df.withColumnRenamed(each,  re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '')))

    for column in column_cast:
        value_type_str = [True if type_col == 'string' else False for name_col, type_col in df.dtypes if column == name_col][0]
        
        if value_type_str:
            df = df.withColumn('upper_' + str(column), upper(col(column)))

    df.show()
    return df

def list_of_strings(arg):
    return arg.split(',')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres.')
    parser.add_argument('--csv_file', help='csv file')
    parser.add_argument('--columns', help='columns', type=list_of_strings)
    args = parser.parse_args()
    main(args)