import os


os.environ['SPARK_HOME'] = "C:\Spark"  
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'  
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab' 
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("pys") \
    .getOrCreate()


data = [("Hari", 30), ("Sri", 25), ("Senthil", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()
