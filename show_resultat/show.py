from pyspark.sql import SparkSession

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("CheckMetrics") \
    .getOrCreate()

# 读取 ph_trend 数据
ph_trend_df = spark.read.format("parquet").load("hdfs://namenode:8020/data/metrics/ph_trend")
if ph_trend_df.count() == 0:
    print("集成数据集为空")
else:    
    print("ph_trend 数据:")
    ph_trend_df.show()  # 显示 ph_trend 结果

# 读取 sulfur_trend 数据
sulfur_trend_df = spark.read.format("parquet").load("hdfs://namenode:8020/data/metrics/sulfur_trend")
if sulfur_trend_df.count() == 0:
    print("集成数据集为空")
else:
    print("sulfur_trend 数据:")
    sulfur_trend_df.show()  # 显示 sulfur_trend 结果
