from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, expr
import pandas

spark = SparkSession.builder.appName("pivot_script_1").getOrCreate()

filePath = "/home/kernelpanic/Documents/SPARK/spark_sql/spark_pivot/BaseDatos.xlsx"

pdf = pandas.read_excel(filePath, sheet_name='1')
df = spark.createDataFrame(pdf)

df2 = df.unpivot(['State or territory','Type', 'Unnamed: 2'],['2018', '2019', '2020', '2021', '2022'], 'year','value')

df2.toPandas().to_csv('model_1.csv')

