import os 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", "true").csv("gs://chicago-data-project-dezoomcamp/Traffic_Crashes_-_Crashes_20240330.csv")

df.write.mode('overwrite').csv("gs://chicago_crash_bronze_layer/Traffic_Crashes_-_Crashes_20240330.csv")
spark.stop()