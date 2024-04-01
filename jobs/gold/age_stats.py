import os 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql.functions import col, count, mean, stddev, min, max


spark = SparkSession.builder.getOrCreate()

people_df = spark.read.parquet("gs://chicago_crash_silver_layer/people.parquet")

# Filter out records where age is greater than or equal to 0 
filtered_people_df = people_df.filter(col("AGE") >= 0)

age_stats = filtered_people_df.select(mean(col("AGE")).alias("mean_age"),
                      stddev(col("AGE")).alias("stddev_age"),
                      min(col("AGE")).alias("min_age"),
                      max(col("AGE")).alias("max_age"))

age_stats.write.mode('overwrite').parquet("gs://chicago_crash_gold_layer/age_stats.parquet")
spark.stop()
