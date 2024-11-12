import os

os.environ['SPARK_HOME'] = "C:\Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD Example").getOrCreate()

numbers = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(numbers)

a = rdd.collect()
print(a)

data = [("John", 28), ("Alice", 32), ("Bob", 29), ("Eve", 35)]
rdd = spark.sparkContext.parallelize(data)
print("All elements of the RDD: ", rdd.collect())

count = rdd.count()
print("Total number of elements in RDD: ", count)

first_element = rdd.first()
print("First element of the RDD: ", first_element)

rdd.foreach(lambda x: print(x))
