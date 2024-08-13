from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, expr
import pandas

spark = SparkSession.builder.appName("pivot_script_2").getOrCreate()

filePath = "/home/kernelpanic/Documents/SPARK/spark_sql/spark_pivot/BaseDatos.xlsx"

pdf = pandas.read_excel(filePath, sheet_name='3')


