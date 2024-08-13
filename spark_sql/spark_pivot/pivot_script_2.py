from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, expr
import pandas

spark = SparkSession.builder.appName("pivot_script_2").getOrCreate()

filePath = "/home/kernelpanic/Documents/SPARK/spark_sql/spark_pivot/BaseDatos.xlsx"

pdf = pandas.read_excel(filePath, sheet_name='2')
columns = pdf.columns.tolist()
columns.remove('Unnamed: 0')
columns.remove('Unnamed: 1')
for column in columns:
    pdf[column] = pdf[column].astype(str)
df = spark.createDataFrame(pdf)

for column in columns:
    df = df.withColumn(column.replace('–','-'), col(column))
#df.show()

unpivotExp = "stack(15,'2008-09',2008-09,'2009-10',2009-10,'2010-11',2010-11,'2011-12',2011-12,'2012-13',2012-13,'2013-14',2013-14,'2014-15',2014-15,'2015-16',2015-16,'2016-17',2016-17,'2017-18',2017-18,'2018-19',2018-19,'2019-20',2019-20,'2020-21',2020-21,'2021-22',2021-22,'2022-23',2022-23) as (year, val)"
df2 = df.select('Unnamed: 0','Unnamed: 1',expr(unpivotExp))
df2.toPandas().to_csv('model_2.csv')
