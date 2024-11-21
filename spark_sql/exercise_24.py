"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-values-per-group-groupBy.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_24')
         .getOrCreate())

nums = spark.range(5).withColumn("group", col("id") % 2)
#nums.show()

nums.registerTempTable("numsTable")
df = spark.sql("""
    select group, max(id)
    from numsTable
    group by group
""")

df.show()