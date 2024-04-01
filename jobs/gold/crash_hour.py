import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("gs://chicago_crash_silver_layer/crash.parquet")

columns = ["hour", "CRASH_RECORD_ID"]
crash_groupby_hour_df = df.groupBy("hour").agg(count("CRASH_RECORD_ID").alias("count"))
crash_groupby_hour_df = crash_groupby_hour_df.orderBy("hour")
crash_groupby_hour_df.show()

crash_groupby_hour_df.write.mode('overwrite').parquet("gs://chicago_crash_gold_layer/crash_groupby_hour.parquet")
spark.stop()