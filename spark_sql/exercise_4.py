from pyspark.sql import SparkSession, HiveContext
# from pyspark.sql.functions import withColumn

spark = (SparkSession
         .builder
         .appName('exersice_4')
         .getOrCreate())

df = spark.range(50).withColumn("key", "id" % 5)

df.show()
# df.registerTempTable("dfTable")
