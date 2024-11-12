import os

os.environ['SPARK_HOME'] = "C:\Spark"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("WordCountDemo").getOrCreate()

lines_rdd = spark.sparkContext.textFile("./data/data.txt")

word_counts_rdd = lines_rdd.flatMap(lambda line: line.split()) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

top_10_rdd = word_counts_rdd.take(10)
print("Top 10 frequent words from RDD:", top_10_rdd)

df = spark.read.text("./data/data.txt")

word_counts_df = df.selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word").count().orderBy(col("count").desc())

top_10_df = word_counts_df.take(10)
print("Top 10 frequent words from DataFrame:", top_10_df)
