import os 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

## Fix Schema 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_timestamp

# Define the schema for the DataFrame
schema = StructType([
    StructField("CRASH_RECORD_ID", StringType(), True),
    StructField("CRASH_DATE_EST_I", StringType(), True),
    StructField("CRASH_DATE", StringType(), True),
    StructField("POSTED_SPEED_LIMIT", IntegerType(), True),
    StructField("TRAFFIC_CONTROL_DEVICE", StringType(), True),
    StructField("DEVICE_CONDITION", StringType(), True),
    StructField("WEATHER_CONDITION", StringType(), True),
    StructField("LIGHTING_CONDITION", StringType(), True),
    StructField("FIRST_CRASH_TYPE", StringType(), True),
    StructField("TRAFFICWAY_TYPE", StringType(), True),
    StructField("LANE_CNT", IntegerType(), True),
    StructField("ALIGNMENT", StringType(), True),
    StructField("ROADWAY_SURFACE_COND", StringType(), True),
    StructField("ROAD_DEFECT", StringType(), True),
    StructField("REPORT_TYPE", StringType(), True),
    StructField("CRASH_TYPE", StringType(), True),
    StructField("INTERSECTION_RELATED_I", StringType(), True),
    StructField("NOT_RIGHT_OF_WAY_I", StringType(), True),
    StructField("HIT_AND_RUN_I", StringType(), True),
    StructField("DAMAGE", StringType(), True),
    StructField("DATE_POLICE_NOTIFIED", TimestampType(), True),
    StructField("PRIM_CONTRIBUTORY_CAUSE", StringType(), True),
    StructField("SEC_CONTRIBUTORY_CAUSE", StringType(), True),
    StructField("STREET_NO", IntegerType(), True),
    StructField("STREET_DIRECTION", StringType(), True),
    StructField("STREET_NAME", StringType(), True),
    StructField("BEAT_OF_OCCURRENCE", IntegerType(), True),
    StructField("PHOTOS_TAKEN_I", StringType(), True),
    StructField("STATEMENTS_TAKEN_I", StringType(), True),
    StructField("DOORING_I", StringType(), True),
    StructField("WORK_ZONE_I", StringType(), True),
    StructField("WORK_ZONE_TYPE", StringType(), True),
    StructField("WORKERS_PRESENT_I", StringType(), True),
    StructField("NUM_UNITS", IntegerType(), True),
    StructField("MOST_SEVERE_INJURY", StringType(), True),
    StructField("INJURIES_TOTAL", IntegerType(), True),
    StructField("INJURIES_FATAL", IntegerType(), True),
    StructField("INJURIES_INCAPACITATING", IntegerType(), True),
    StructField("INJURIES_NON_INCAPACITATING", IntegerType(), True),
    StructField("INJURIES_REPORTED_NOT_EVIDENT", IntegerType(), True),
    StructField("INJURIES_NO_INDICATION", IntegerType(), True),
    StructField("INJURIES_UNKNOWN", IntegerType(), True),
    StructField("CRASH_HOUR", IntegerType(), True),
    StructField("CRASH_DAY_OF_WEEK", IntegerType(), True),
    StructField("CRASH_MONTH", IntegerType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("LOCATION", StringType(), True)
])


# # Now you can use this schema when reading the CSV file
# df = spark.read.option("header", "true").schema(schema).csv("Traffic_Crashes_-_Crashes_20240330.csv")

# # Show the DataFrame schema and some sample data
# df.printSchema()
# df.show(5)  # Show the first 5 rows

def parse_crash_date(date_str):
    try:
        return to_timestamp(date_str, "MM/dd/yyyy hh:mm:ss a")
    except:
        return None

# Read CSV file with defined schema
df = spark.read.option("header", "true").schema(schema).csv("gs://chicago_crash_bronze_layer/Traffic_Crashes_-_Crashes_20240330.csv")

# Parse CRASH_DATE column
df = df.withColumn("CRASH_DATE", parse_crash_date(col("CRASH_DATE")))

# Show the DataFrame schema and some sample data
df.printSchema()

# Filter columns where all values are null
columns_to_keep = [col_name for col_name in df.columns if df.where(col(col_name).isNotNull()).count() > 0]

# Select only the columns to keep
df_filtered = df.select(*columns_to_keep)

# Define the threshold for non-null values per row
thresh = 2

# # Drop rows with fewer than 2 non-null values
df = df_filtered.na.drop(thresh=thresh)
df.printSchema()
df.show(2)
from pyspark.sql.functions import col, to_date, year, month, hour

df = df.withColumn('date', to_date(col('CRASH_DATE')))
df = df.withColumn('month', month(col('CRASH_DATE')))
df = df.withColumn('year', year(col('CRASH_DATE')))
df = df.withColumn('hour', hour(col('CRASH_DATE')))

df.write.mode('overwrite').parquet("gs://chicago_crash_silver_layer/crash.parquet")
spark.stop()