from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, split, regexp_replace
from pyspark.sql.types import StringType
import pandas

spark = SparkSession.builder.appName("desempleadosAU").getOrCreate()

filePath = "/home/kernelpanic/Documents/SPARK/spark_sql/spark_pivot/desempleadosAU.xlsx"

pdf = pandas.read_excel(filePath, sheet_name='Data3')

columns = pdf.columns.tolist()

Unit = pdf.loc[0, :].values.tolist()
Series_Type = pdf.loc[1, :].values.tolist()
Data_Type = pdf.loc[2, :].values.tolist()
Frequency = pdf.loc[3, :].values.tolist()
Collection_Month = pdf.loc[4, :].values.tolist()
Series_Start = pdf.loc[5, :].values.tolist()
Series_End = pdf.loc[6, :].values.tolist()
No_Obs = pdf.loc[7, :].values.tolist()
Series_ID = pdf.loc[8, :].values.tolist()

columns.remove('Unnamed: 0')
Unit.remove('Unit')
Series_Type.remove('Series Type')
Data_Type.remove('Data Type')
Frequency.remove('Frequency')
Collection_Month.remove('Collection Month')
Series_Start.remove('Series Start')
Series_End.remove('Series End')
No_Obs.remove('No. Obs')
Series_ID.remove('Series ID')

header = list(zip(columns, Unit, Series_Type, Data_Type, Frequency, Collection_Month, Series_Start, Series_End, No_Obs, Series_ID))
header_list = [list(pair) for pair in header]

column_name_list = []
for column in header_list:
    column_0 = column[0].replace('.1','').replace('.2','')
    column_name = f'{str(column_0)},{str(column[1])},{str(column[2])},{str(column[3])},{str(column[4])},{str(column[5])},{str(column[6])},{str(column[7])},{str(column[8])},{str(column[9])}' 
    column_name_list.append(column_name)
    pdf.rename(columns={column[0]:  column_name}, inplace=True)


df = spark.createDataFrame(pdf)

depreacted_rows = [
    'Unit',
    'Series Type',
    'Data Type',
    'Frequency',
    'Collection Month',
    'Series Start',
    'Series End',
    'No. Obs',
    'Series ID'
]
df = df.filter(~df['Unnamed: 0'].isin(depreacted_rows))

#print(column_name_list)

df2 = df.unpivot('Unnamed: 0',
        column_name_list,
        'compose_value','value')

df2 = df2.withColumn('description',split(df2['compose_value'],',').getItem(0))
df2 = df2.withColumn('Unit',split(df2['compose_value'],',').getItem(1))
df2 = df2.withColumn('Series_Type',split(df2['compose_value'],',').getItem(2))
df2 = df2.withColumn('Data_Type',split(df2['compose_value'],',').getItem(3))
df2 = df2.withColumn('Frequency',split(df2['compose_value'],',').getItem(4))
df2 = df2.withColumn('Collection_Month',split(df2['compose_value'],',').getItem(5))
df2 = df2.withColumn('Series_Start',split(df2['compose_value'],',').getItem(6))
df2 = df2.withColumn('Series_End',split(df2['compose_value'],',').getItem(7))
df2 = df2.withColumn('No_Obs',split(df2['compose_value'],',').getItem(8))
df2 = df2.withColumn('Series_ID',split(df2['compose_value'],',').getItem(9))



pattern = r'(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+).+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+'

df2 = df2.withColumn('Unnamed: 0', regexp_replace('Unnamed: 0', pattern, '$1-$2-$3 $4:$5:$6').cast('timestamp'))

df2 = df2.drop('compose_value')

#df2 = df2.drop('Unnamed: 0')

#df2.show()

df2.toPandas().to_csv('model_desempleadosAU.csv')
