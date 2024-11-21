from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("ReadParquetFile") \
    .getOrCreate()


parquet_df = spark.read.parquet("hdfs://namenode:8020/output/integrated_data_v1")


parquet_df.show(truncate=False)


parquet_df.printSchema()