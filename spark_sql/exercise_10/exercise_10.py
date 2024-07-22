"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-Ids-of-Rows-with-Word-in-Array-Column.html
"""
from pyspark.sql.functions import col, explode,array_agg
from pyspark.sql import SparkSession, HiveContext

spark = (SparkSession
         .builder
         .appName('exersice_10')
         .getOrCreate())

df = spark.read.option("header", True).csv('input.csv')

df.registerTempTable("dfTable")

df2 = spark.sql("""
                with words_cte as (
                    select 
                        word as number_w
                    from dfTable
                ), 
                final as (
                    select 
                        df.id,
                        df.words,
                        w.number_w,
                        case 
                            when words like '%'||number_w||'%' then True
                            else False
                        end result
                    from dfTable df
                    cross join words_cte w
                )
                select 
                    number_w as w,
                    id
                from final
                where result = True
                order by number_w
            """)

df2.groupBy('w').agg(array_agg('id').alias('row')).orderBy('w').show()