import os 
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

## Fix Schema 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, to_timestamp

# Define the schema for the DataFrame
vehicle_schema = StructType([
    StructField("CRASH_UNIT_ID", IntegerType(), True),
    StructField("CRASH_RECORD_ID", StringType(), True),
    StructField("CRASH_DATE", StringType(), True),
    StructField("UNIT_NO", IntegerType(), True),
    StructField("UNIT_TYPE", StringType(), True),
    StructField("NUM_PASSENGERS", IntegerType(), True),
    StructField("VEHICLE_ID", StringType(), True),
    StructField("CMRC_VEH_I", StringType(), True),
    StructField("MAKE", StringType(), True),
    StructField("MODEL", StringType(), True),
    StructField("LIC_PLATE_STATE", StringType(), True),
    StructField("VEHICLE_YEAR", IntegerType(), True),
    StructField("VEHICLE_DEFECT", StringType(), True),
    StructField("VEHICLE_TYPE", StringType(), True),
    StructField("VEHICLE_USE", StringType(), True),
    StructField("TRAVEL_DIRECTION", StringType(), True),
    StructField("MANEUVER", StringType(), True),
    StructField("TOWED_I", StringType(), True),
    StructField("FIRE_I", StringType(), True),
    StructField("OCCUPANT_CNT", IntegerType(), True),
    StructField("EXCEED_SPEED_LIMIT_I", StringType(), True),
    StructField("TOWED_BY", StringType(), True),
    StructField("TOWED_TO", StringType(), True),
    StructField("AREA_00_I", StringType(), True),
    StructField("AREA_01_I", StringType(), True),
    StructField("AREA_02_I", StringType(), True),
    StructField("AREA_03_I", StringType(), True),
    StructField("AREA_04_I", StringType(), True),
    StructField("AREA_05_I", StringType(), True),
    StructField("AREA_06_I", StringType(), True),
    StructField("AREA_07_I", StringType(), True),
    StructField("AREA_08_I", StringType(), True),
    StructField("AREA_09_I", StringType(), True),
    StructField("AREA_10_I", StringType(), True),
    StructField("AREA_11_I", StringType(), True),
    StructField("AREA_12_I", StringType(), True),
    StructField("AREA_99_I", StringType(), True),
    StructField("FIRST_CONTACT_POINT", StringType(), True),
    StructField("CMV_ID", IntegerType(), True),
    StructField("USDOT_NO", StringType(), True),
    StructField("CCMC_NO", StringType(), True),
    StructField("ILCC_NO", StringType(), True),
    StructField("COMMERCIAL_SRC", StringType(), True),
    StructField("GVWR", StringType(), True),
    StructField("CARRIER_NAME", StringType(), True),
    StructField("CARRIER_STATE", StringType(), True),
    StructField("CARRIER_CITY", StringType(), True),
    StructField("HAZMAT_PLACARDS_I", StringType(), True),
    StructField("HAZMAT_NAME", StringType(), True),
    StructField("UN_NO", StringType(), True),
    StructField("HAZMAT_PRESENT_I", StringType(), True),
    StructField("HAZMAT_REPORT_I", StringType(), True),
    StructField("HAZMAT_REPORT_NO", StringType(), True),
    StructField("MCS_REPORT_I", StringType(), True),
    StructField("MCS_REPORT_NO", StringType(), True),
    StructField("HAZMAT_VIO_CAUSE_CRASH_I", StringType(), True),
    StructField("MCS_VIO_CAUSE_CRASH_I", StringType(), True),
    StructField("IDOT_PERMIT_NO", StringType(), True),
    StructField("WIDE_LOAD_I", StringType(), True),
    StructField("TRAILER1_WIDTH", StringType(), True),
    StructField("TRAILER2_WIDTH", StringType(), True),
    StructField("TRAILER1_LENGTH", IntegerType(), True),
    StructField("TRAILER2_LENGTH", IntegerType(), True),
    StructField("TOTAL_VEHICLE_LENGTH", IntegerType(), True),
    StructField("AXLE_CNT", IntegerType(), True),
    StructField("VEHICLE_CONFIG", StringType(), True),
    StructField("CARGO_BODY_TYPE", StringType(), True),
    StructField("LOAD_TYPE", StringType(), True),
    StructField("HAZMAT_OUT_OF_SERVICE_I", StringType(), True),
    StructField("MCS_OUT_OF_SERVICE_I", StringType(), True),
    StructField("HAZMAT_CLASS", StringType(), True)
])

def parse_crash_date(date_str):
    try:
        return to_timestamp(date_str, "MM/dd/yyyy hh:mm:ss a")
    except:
        return None

# Now you can use this schema when reading the CSV file
vehicle_df = spark.read.option("header", "true").schema(vehicle_schema).csv("gs://chicago_crash_bronze_layer/Traffic_Crashes_-_Vehicles_20240330.csv")
vehicle_df = vehicle_df.withColumn("CRASH_DATE", parse_crash_date(col("CRASH_DATE")))

# Show the DataFrame schema and some sample data
vehicle_df.printSchema()
# Filter columns where all values are null
columns_to_keep = [col_name for col_name in vehicle_df.columns if vehicle_df.where(col(col_name).isNotNull()).count() > 0]

# Select only the columns to keep
df_filtered = vehicle_df.select(*columns_to_keep)

# Define the threshold for non-null values per row
thresh = 2

# Drop rows with fewer than 2 non-null values
df = df_filtered.na.drop(thresh=thresh)

from pyspark.sql.functions import col, to_date, year, month, hour

df = df.withColumn('date', to_date(col('CRASH_DATE')))
df = df.withColumn('month', month(col('CRASH_DATE')))
df = df.withColumn('year', year(col('CRASH_DATE')))
df = df.withColumn('hour', hour(col('CRASH_DATE')))

df.write.mode('overwrite').parquet("gs://chicago_crash_silver_layer/vehicle.parquet")
spark.stop()