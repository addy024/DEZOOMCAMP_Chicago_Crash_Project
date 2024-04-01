import os 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

## Fix Schema 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_timestamp


# Define the schema for the DataFrame
people_schema = StructType([
    StructField("PERSON_ID", StringType(), True),
    StructField("PERSON_TYPE", StringType(), True),
    StructField("CRASH_RECORD_ID", StringType(), True),
    StructField("VEHICLE_ID", StringType(), True),
    StructField("CRASH_DATE", StringType(), True),
    StructField("SEAT_NO", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("ZIPCODE", StringType(), True),
    StructField("SEX", StringType(), True),
    StructField("AGE", IntegerType(), True),
    StructField("DRIVERS_LICENSE_STATE", StringType(), True),
    StructField("DRIVERS_LICENSE_CLASS", StringType(), True),
    StructField("SAFETY_EQUIPMENT", StringType(), True),
    StructField("AIRBAG_DEPLOYED", StringType(), True),
    StructField("EJECTION", StringType(), True),
    StructField("INJURY_CLASSIFICATION", StringType(), True),
    StructField("HOSPITAL", StringType(), True),
    StructField("EMS_AGENCY", StringType(), True),
    StructField("EMS_RUN_NO", StringType(), True),
    StructField("DRIVER_ACTION", StringType(), True),
    StructField("DRIVER_VISION", StringType(), True),
    StructField("PHYSICAL_CONDITION", StringType(), True),
    StructField("PEDPEDAL_ACTION", StringType(), True),
    StructField("PEDPEDAL_VISIBILITY", StringType(), True),
    StructField("PEDPEDAL_LOCATION", StringType(), True),
    StructField("BAC_RESULT", StringType(), True),
    StructField("BAC_RESULT_VALUE", DoubleType(), True),
    StructField("CELL_PHONE_USE", StringType(), True)
])

def parse_crash_date(date_str):
    try:
        return to_timestamp(date_str, "MM/dd/yyyy hh:mm:ss a")
    except:
        return None


people_df = spark.read.option("header", "true").schema(people_schema).csv("gs://chicago_crash_bronze_layer/Traffic_Crashes_-_People_20240330.csv")

people_df = people_df.withColumn("CRASH_DATE", parse_crash_date(col("CRASH_DATE")))

columns_to_keep = [col_name for col_name in people_df.columns if people_df.where(col(col_name).isNotNull()).count() > 0]

# Select only the columns to keep
df_filtered = people_df.select(*columns_to_keep)

# Define the threshold for non-null values per row
thresh = 2

# Drop rows with fewer than 2 non-null values
df = df_filtered.na.drop(thresh=thresh)

from pyspark.sql.functions import col, to_date, year, month, hour

df = df.withColumn('date', to_date(col('CRASH_DATE')))
df = df.withColumn('month', month(col('CRASH_DATE')))
df = df.withColumn('year', year(col('CRASH_DATE')))
df = df.withColumn('hour', hour(col('CRASH_DATE')))

df.write.mode('overwrite').parquet("gs://chicago_crash_silver_layer/people.parquet")
spark.stop()