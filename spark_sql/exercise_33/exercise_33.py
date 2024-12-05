"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/sql/Finding-1st-and-2nd-Bestsellers-Per-Genre.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext('local', 'Simple App')

spark = (SparkSession
         .builder
         .appName('exercise_33')
         .getOrCreate())

salaries = spark.read.options(delimiter=',',inferSchema=True,header=True).csv('data.csv')

salaries.registerTempTable('salariesTable')

result = spark.sql("""
    with cte_max_salary as (
        select 
            department,
            max(salary) max_salary
        from salariesTable
        group by department
    ),
    cte_salaries as (
        select 
            id,
            name,
            department,
            salary
        from salariesTable
    ),
    final as (
        select 
            cte_salaries.id,
            cte_salaries.name,
            cte_salaries.department,
            cte_salaries.salary,
            cte_max_salary.max_salary - cte_salaries.salary diff
        from cte_salaries
        inner join cte_max_salary
            on cte_salaries.department = cte_max_salary.department
        order by cte_salaries.department
    )
    select * from final
    """)

result.show()