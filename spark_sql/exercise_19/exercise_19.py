"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-counting-occurences-of-years-and-months-for-past-24-months.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_19')
         .getOrCreate())

df = spark.read.options(delimiter=',',header=True,inferSchema=True).csv('sales.csv')


df.registerTempTable("dfTable")

df2 = spark.sql("""
                with date_cast as (
                    select 
                        YEAR_MONTH,
                        AMOUNT
                    from dfTable
                ),
                date_range as 
                (
                    select date_format(add_months(concat(date_format(current_date,'yyyy-MM'),'-01'),-s.i),'yyyyMM') as year_month 
                    from ( 
                        select posexplode(split(space(24),' ')) as (i,x) 
                    ) s 
                ),
                final as (
                    select 
                        dr.year_month,
                        sum(coalesce(dc.amount,0)) amount
                    from date_range dr
                    left join date_cast dc
                        on dc.YEAR_MONTH = dr.year_month
                    where to_date(dr.year_month||'01','yyyyMMdd') >= to_date(dateadd(year, -2, current_date))
                    group by dr.year_month
                    order by dr.year_month desc
                )
                select * from final
            """)

df2.show()