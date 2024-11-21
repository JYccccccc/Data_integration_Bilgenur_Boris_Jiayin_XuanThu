from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType

# Creer SparkSession
streaming_df = SparkSession.builder \
    .appName("AcidifiedSurfaceWaterStreamProcessor") \
    .getOrCreate()

# Definir les configurations Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'acidified-water-topic'

# Definir le schema des données
schema = StructType([
    StructField("SITE_ID", StringType(), True),
    StructField("PROGRAM_ID", StringType(), True),
    StructField("DATE_SMP", TimestampType(), True),
    StructField("SAMPLE_LOCATION", StringType(), True),
    StructField("SAMPLE_TYPE", StringType(), True),
    StructField("WATERBODY_TYPE", StringType(), True),
    StructField("SAMPLE_DEPTH", FloatType(), True),
    StructField("TIME_SMP", StringType(), True),
    StructField("ANC_UEQ_L", FloatType(), True),
    StructField("CA_UEQ_L", FloatType(), True),
    StructField("CHL_A_UG_L", FloatType(), True),
    StructField("CL_UEQ_L", FloatType(), True),
    StructField("COND_UM_CM", FloatType(), True),
    StructField("DOC_MG_L", FloatType(), True),
    StructField("F_UEQ_L", FloatType(), True),
    StructField("K_UEQ_L", FloatType(), True),
    StructField("MG_UEQ_L", FloatType(), True),
    StructField("NA_UEQ_L", FloatType(), True),
    StructField("NH4_UEQ_L", FloatType(), True),
    StructField("NO3_UEQ_L", FloatType(), True),
    StructField("N_TD_UEQ_L", FloatType(), True),
    StructField("PH_EQ", FloatType(), True),
    StructField("PH_FLD", FloatType(), True),
    StructField("PH_LAB", FloatType(), True),
    StructField("PH_STVL", FloatType(), True),
    StructField("P_TL_UEQ_L", FloatType(), True),
    StructField("SECCHI_M", FloatType(), True),
    StructField("SIO2_MG_L", FloatType(), True),
    StructField("SO4_UEQ_L", FloatType(), True),
    StructField("WTEMP_DEG_C", FloatType(), True)
])


# Obtenir les données en streaming depuis Kafka
kafka_stream = streaming_df \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# transformer les données en JSON
parsed_df = kafka_stream \
    .selectExpr("CAST(value AS STRING) AS raw_value") \
    .select(from_csv(col("raw_value"), schema).alias("data")) \
    .select("data.*")
# Afficher les données en streaming
query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()
