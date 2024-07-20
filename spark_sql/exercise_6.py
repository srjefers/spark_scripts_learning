from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import first

spark = (SparkSession
         .builder
         .appName('exersice_3')
         .getOrCreate())

data = [
    ("100", "John", 35, None),
    ("100", "John", None, "Georgia"),
    ("101", "Mike", 25, None),
    ("101", "Mike", None, "New York"),
    ("103", "Mary", 22, None),
    ("103", "Mary", None, "Texas"),
    ("104", "Smith", 25, None),
    ("105", "Jake", None, "Florida")
]

df = spark.createDataFrame(data,["id", "name", "age", "city"])

df.groupBy(['id','name']).agg(first('age',ignorenulls=True),first('city',ignorenulls=True)).show()

