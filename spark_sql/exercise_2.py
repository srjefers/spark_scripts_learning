from pyspark.sql import SparkSession, HiveContext

spark = (SparkSession
         .builder
         .appName('exersice_2')
         .getOrCreate())

df = spark.createDataFrame([(1, "MV1"),
  (1, "MV2"),
  (2, "VPV"),
  (2, "Others")],["id", "value"])

df.registerTempTable("dfTable")

df2 = spark.sql("""
                with table as (
                    SELECT 
                        id, value
                    FROM dfTable
                    WHERE value <> 'Others'
                ),
                filter as (
                    SELECT 
                        id, value, RIGHT(value, 1) vlv
                    FROM table
                )
                SELECT 
                    id, value
                FROM filter
                WHERE vlv <> '2'
                """)

df2.show()