"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-Longest-Sequence.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, col, count

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_41')
         .getOrCreate())

df = spark.read.options(delimiter=',', inferSchema=True, header=True).csv('data.csv')

df.registerTempTable('dfTable')
spark.sql("""
    with grp_cte as (
        select 
            id, time,
            time - row_number() over (partition by id order by time) as grp
        from dfTable           
    ),
    final as (
        select 
            id, count(grp) cnt
        from grp_cte
        group by id, grp
    )
    select 
        id, max(cnt) time
    from final
    group by id
""").show()

