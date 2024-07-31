"""
    url: https://nxcals-docs.web.cern.ch/current/user-guide/examples/standalone-app/
"""
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

conf = (SparkConf()
        .setAppName("exersice_16")
        .setMaster('yarn')
        .set('spark.driver.memory', '4G')
        )
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

