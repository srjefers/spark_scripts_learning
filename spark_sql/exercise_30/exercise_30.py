"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Generating-Exam-Assessment-Report.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, first,lit, col

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_30')
         .getOrCreate())

df = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')

df = df.withColumn('_Qid',concat(lit('Qid_'),df.Qid))

df = df.groupBy(['ParticipantID','Assessment','GeoTag']).pivot('_Qid').agg(first('AnswerText'))

df.show()