from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("CheckMetrics") \
    .getOrCreate()


ph_trend_df = spark.read.format("parquet").load("hdfs://namenode:8020/data/metrics/ph_trend")
if ph_trend_df.count() == 0:
    print("Vide")
else:    
    print("ph_trend data:")
    ph_trend_df.show() 


sulfur_trend_df = spark.read.format("parquet").load("hdfs://namenode:8020/data/metrics/sulfur_trend")
if sulfur_trend_df.count() == 0:
    print("Vide")
else:
    print("sulfur_trend data:")
    sulfur_trend_df.show()  
