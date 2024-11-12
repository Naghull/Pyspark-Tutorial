import os
os.environ['SPARK_HOME'] = "C:\Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext(appName="SparkApp")

sc
sc.stop()


spark = SparkSession.builder \
    .appName("SparkApp") \
    .getOrCreate()


sc = spark.sparkContext

sc 
sc.stop()
