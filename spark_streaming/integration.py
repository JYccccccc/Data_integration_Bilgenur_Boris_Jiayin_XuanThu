from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col

# Initiation SparkSession
spark = SparkSession.builder \
    .appName("DataIntegration") \
    .getOrCreate()

# Configure Kafka
bootstrap_servers = "localhost:9092"
stream_data_topic = "acidified-water-topic"

# Configure HDFS paths
methods_path = "hdfs://namenode:8020/data/Methods_2022_8_1.xlsx"
site_info_path = "hdfs://namenode:8020/data/Site_Information_2022_8_1.xlsx"
output_path = "hdfs://namenode:8020/output/integrated_data_v1"

# Define schemas for the data
methods_schema = StructType([
    StructField("PROGRAM_ID", StringType(), True),
    StructField("PARAMETER", StringType(), True),
    StructField("METHOD", StringType(), True),
    StructField("METHOD_DESCRIPTION", StringType(), True)
])

site_info_schema = StructType([
    StructField("SITE_ID", StringType(), True),
    StructField("SITE_NAME", StringType(), True),
    StructField("REGION", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("PROGRAM_ID", StringType(), True)  # Added PROGRAM_ID for join
])

# Stream data schema (based on LTM_Data)
stream_data_schema = StructType([
    StructField("PROGRAM_ID", StringType(), True),
    StructField("PARAMETER", StringType(), True),
    StructField("VALUE", DoubleType(), True),
    StructField("UNIT", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("SITE_ID", StringType(), True)
])

# Read static data from HDFS
methods_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .schema(methods_schema) \
    .load(methods_path)

site_info_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .schema(site_info_schema) \
    .load(site_info_path)

# Read streaming data from Kafka
streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", stream_data_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr(
        "split(value, ',')[0] AS PROGRAM_ID",
        "split(value, ',')[1] AS PARAMETER",
        "split(value, ',')[2] AS VALUE",
        "split(value, ',')[3] AS UNIT",
        "split(value, ',')[4] AS DATE",
        "split(value, ',')[5] AS SITE_ID"
    ).withColumn("VALUE", col("VALUE").cast(DoubleType()))

# Merge streaming data with static data
# Join by PROGRAM_ID and PARAMETER from methods_df, and SITE_ID from site_info_df
integrated_df = streaming_df \
    .join(methods_df, on=["PROGRAM_ID", "PARAMETER"], how="left") \
    .join(site_info_df, on=["SITE_ID", "PROGRAM_ID"], how="left")

# Write the integrated data to HDFS in Parquet format
query = integrated_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "hdfs://localhost:8020/output/checkpoints") \
    .start()

query.awaitTermination()