from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import split
#import pyspark.sql.functions as F


spark = (SparkSession
         .builder
         .appName('exersice_1')
         .getOrCreate())

#sqlCtx= HiveContext(spark)

df = spark.createDataFrame([("50000.0#0#0#", "#"),
  ("0@1000.0@", "@"),
  ("1$", "$"),
  ("1000.00^Test_string", "^")],["VALUES", "Delimiter"])

df.registerTempTable("cleanTable")

df2 = spark.sql("""
                SELECT 
                    VALUES, cast(Delimiter as string),
                    split(cast(VALUES as string), cast(Delimiter as string)) as split_values
                FROM cleanTable
                """)

df2.show(truncate = False)
