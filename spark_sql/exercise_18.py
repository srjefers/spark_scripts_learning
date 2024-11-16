"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Difference-in-Days-Between-Dates-As-Strings.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date, datediff
from pyspark.sql.types import StringType

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_18')
         .getOrCreate())

"""
val dates = Seq(
   "08/11/2015",
   "09/11/2015",
   "09/12/2015").toDF("date_string")
dates.show
"""
dates = [
    ("08/11/2015"),
    ("09/11/2015"),
    ("09/12/2015")
]

df = spark.createDataFrame(dates,StringType())
df = df.withColumn('date_string', col('value'))

df = df.select(df.date_string,to_date(df.date_string, 'dd/MM/yyyy').alias("to_date"),datediff(current_date(),to_date(df.date_string, 'dd/MM/yyyy')).alias("diff"))

df.show()