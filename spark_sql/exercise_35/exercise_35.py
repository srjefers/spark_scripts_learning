"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Calculating-Difference-Between-Consecutive-Rows-Per-Window.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext('local','Simple App')

spark = (SparkSession
         .builder
         .appName('exercise_35')
         .getOrCreate())

df = spark.read.options(delimiter=',', inferSchema=True, header=True).csv('data.csv')

df.registerTempTable('dfTable')
df = spark.sql("""
    with final as (
        select 
            time, department, items_sold,
            running_total,
            running_total - coalesce(lag(running_total) over (partition by department order by time),0) diff
        from dfTable
    )
    select 
        time, department, items_sold,
        running_total, diff
    from final
""")

df.show()