"""
    url: https://nxcals-docs.web.cern.ch/current/user-guide/examples/standalone-app/
"""
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col
import re


def main():
    csv_file = 'input.csv'
    column_cast = ['city']
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

    return df


if __name__ == '__main__':
    main()