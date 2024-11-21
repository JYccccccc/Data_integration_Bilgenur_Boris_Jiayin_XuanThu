from pyspark.sql import SparkSession

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("ReadParquetFile") \
    .getOrCreate()

# 读取 Parquet 文件
parquet_df = spark.read.parquet("hdfs://namenode:8020/output/integrated_data_v1")

# 显示前几行数据
parquet_df.show(truncate=False)

# 显示数据的架构
parquet_df.printSchema()