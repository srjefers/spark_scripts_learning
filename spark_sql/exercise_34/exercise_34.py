"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Calculating-Running-Total-Cumulative-Sum.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext('local','Simple App')

spark = (SparkSession
         .builder
         .appName('exercise_34')
         .getOrCreate())

df = spark.read.options(delimiter=',', inferSchema=True, header=True).csv('data.csv')

df.registerTempTable('dfTable')

df = spark.sql("""
    with final as (
        select 
            time, department, items_sold,
            sum(items_sold) over (partition by department order by time) running_total
        from dfTable
    )
    select 
        time, department, items_sold,
        running_total
    from final
""")
df.show()