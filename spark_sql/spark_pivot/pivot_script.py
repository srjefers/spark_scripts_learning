from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col, posexplode, monotonically_increasing_id, expr
import pandas

spark = SparkSession.builder.appName("Test").getOrCreate()

filePath = "/home/kernelpanic/Documents/SPARK/spark_sql/spark_pivot/BaseDatos.xlsx"

pdf = pandas.read_excel(filePath, sheet_name='1')
df = spark.createDataFrame(pdf)

#df = df.withColumn('id_c', monotonically_increasing_id())
df.show()

unpivotExp = "stack(5, '2018', 2018, '2019', 2019, '2020', 2020, '2021', 2021, '2022', 2022) as (year, val)"
df.select('State or territory','Type', 'Unnamed: 2',expr(unpivotExp)).show()


