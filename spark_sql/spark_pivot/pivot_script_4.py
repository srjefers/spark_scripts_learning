from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, expr
import pandas

spark = SparkSession.builder.appName("pivot_script_2").getOrCreate()

filePath = "/home/kernelpanic/Documents/SPARK/spark_sql/spark_pivot/BaseDatos.xlsx"

pdf = pandas.read_excel(filePath, sheet_name='4')

for column in columns:
    df = df.withColumn(column.replace('.','-'), col(column))

df = spark.createDataFrame(pdf)

#df.show()
df2 = df.unpivot(['Year','State/Territory','Sex','Age'],['Uncapped Participation Rate: Full-time and Part-time Students','Uncapped Participation Rate: Full-time Students'], 'type', 'val')
df2.toPandas().to_csv('model_4.csv')

