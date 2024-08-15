from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, split
import pandas

spark = SparkSession.builder.appName("pivot_script_2").getOrCreate()

filePath = "/home/kernelpanic/Documents/SPARK/spark_sql/spark_pivot/BaseDatos.xlsx"

pdf = pandas.read_excel(filePath, sheet_name='3')

columns = pdf.columns.tolist()
sex = pdf.loc[0, :].values.tolist()

columns.remove('Unnamed: 0')
sex.remove('Cause of death and ICD-10 code')

header = list(zip(columns, sex))
header_list = [list(pair) for pair in header]



for column in header_list:
    str_column = str(column[0]).replace('.','_')
    pdf.rename(columns={column[0]: f'{str_column},{column[1]}' }, inplace=True)

#print(pdf.columns.tolist())

df = spark.createDataFrame(pdf)
df = df.withColumn('index', monotonically_increasing_id())

df = df.filter(~df.index.isin([0]))
df = df.drop('index')

#df.show()
df2 = df.unpivot('Unnamed: 0',['2013,Males', '2013_1,Females', '2013_2,Persons', '2014,Males', '2014_1,Females', '2014_2,Persons', '2015,Males', '2015_1,Females', '2015_2,Persons', '2016,Males', '2016_1,Females', '2016_2,Persons', '2017,Males', '2017_1,Females', '2017_2,Persons', '2018,Males', '2018_1,Females', '2018_2,Persons', '2019,Males', '2019_1,Females', '2019_2,Persons', '2020,Males', '2020_1,Females', '2020_2,Persons', '2021,Males', '2021_1,Females', '2021_2,Persons', '2022,Males', '2022_1,Females', '2022_2,Persons'],'year','value')
df2 = df2.withColumn('sex',split(df2['year'],',').getItem(1))
df2 = df2.withColumn('year',split(df2['year'],',').getItem(0))

#df2.show()
df2.toPandas().to_csv('model_3.csv')