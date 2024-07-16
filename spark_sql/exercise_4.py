from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col, collect_set, udf
# collect_set

spark = (SparkSession
         .builder
         .appName('exersice_4')
         .getOrCreate())

df = spark.range(50).withColumn("key", col("id") % 5)

split_row = udf(lambda row: row[:3])

df2 = df.groupBy('key').agg(collect_set('id').alias('all'))
df2 = df2.withColumn('only_first_three', split_row(col('all')))

df2.show()
