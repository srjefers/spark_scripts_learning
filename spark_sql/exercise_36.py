"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Converting-Arrays-of-Strings-to-String.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
from pyspark.sql.functions import concat_ws, col

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_36')
         .getOrCreate())

"""
val words = Seq(Array("hello", "world")).toDF("words")
"""
wordsSchema = StructType([
    StructField("words", ArrayType(StringType()), nullable=False)
])

words = spark.createDataFrame([
    [("hello", "world")],
],schema=wordsSchema)

words.select(words['words'], concat_ws(' ',words['words'].getItem(0),words['words'].getItem(1)).alias('solution')).show()


