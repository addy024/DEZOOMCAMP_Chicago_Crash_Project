import os 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

people_df = spark.read.parquet("gs://chicago_crash_silver_layer/people.parquet")


# Count the number of males and females 3
gender_counts = people_df.groupBy("SEX").agg(F.count("*").alias("count"))

gender_counts.write.mode('overwrite').parquet("gs://chicago_crash_gold_layer/gender_counts.parquet")
spark.stop()
