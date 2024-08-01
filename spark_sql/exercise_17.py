"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Using-explode-Standard-Function.html
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,ArrayType, IntegerType

spark = (SparkSession
         .builder
         .appName('exersice_17')
         .getOrCreate())

nums = ArrayType([
    StructField("elemnt", IntegerType(), nullable=False),
])

df = spark.createDataFrame([[1,2,3]],['nums'],schema=MovieCriticsSchema)

df.printSchema()
df.show()