"""
    url: https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-Most-Common-Non-null-Prefix-Occurences-per-Group.html
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext("local", "Simple App")

spark = (SparkSession
         .builder
         .appName('exersice_42')
         .getOrCreate())

df = spark.createDataFrame([
    (1, "Mr"),
    (1, "Mme"),
    (1, "Mr"),
    (1, None),
    (1, None),
    (1, None),
    (2, "Mr"),
    (3, None)
],["UNIQUE_GUEST_ID","PREFIX"])

df.registerTempTable("dfTable")
spark.sql("""
    with dftable_cte as (
        select 
            UNIQUE_GUEST_ID, PREFIX, 
            case when PREFIX is null then 0 else count(1) end CNT
        from dfTable
        group by UNIQUE_GUEST_ID, PREFIX
    ),
    max_records as (
        select 
            UNIQUE_GUEST_ID, max(cnt) MAX_CNT
        from dftable_cte
        group by UNIQUE_GUEST_ID
    ),
    final as (
        select 
            distinct max_records.UNIQUE_GUEST_ID,
            dftable_cte.PREFIX
        from max_records
        left join dftable_cte on dftable_cte.UNIQUE_GUEST_ID = max_records.UNIQUE_GUEST_ID and dftable_cte.CNT = max_records.MAX_CNT
    )
    select * from final
""").show()