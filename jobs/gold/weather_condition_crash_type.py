from pyspark.sql.window import Window 
import os 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("gs://chicago_crash_silver_layer/crash.parquet")

grouped_df = df.groupBy('FIRST_CRASH_TYPE', 'WEATHER_CONDITION') \
                       .agg(F.count('*').alias('COUNT'))

# Window specification to partition by 'FIRST_CRASH_TYPE' and order by count descending
window_spec = Window.partitionBy('FIRST_CRASH_TYPE') \
                    .orderBy(F.desc('COUNT'))

# Add row number to each partition
ranked_df = grouped_df.withColumn('rank', F.row_number().over(window_spec))

# Filter rows with rank = 1 to get the most frequent 'WEATHER_CONDITION' for each 'FIRST_CRASH_TYPE'
result_df = ranked_df.filter(F.col('rank') == 1) \
                     .select('FIRST_CRASH_TYPE', 'WEATHER_CONDITION', 'COUNT') \
                     .withColumnRenamed('WEATHER_CONDITION', 'WEATHER')

# Show the result
result_df.write.mode('overwrite').parquet("gs://chicago_crash_gold_layer/weather_condition_crash_type.parquet")
spark.stop()

