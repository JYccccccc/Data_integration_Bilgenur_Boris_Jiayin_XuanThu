from pyspark.sql import SparkSession

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("ReadExcelFromHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

# Excel文件路径
excel_file_path = "hdfs://namenode:8020/data/Site_Information_2022_8_1.xlsx"

# 读取Excel文件
excel_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Sheet 1'!A1") \
    .load(excel_file_path)

# 显示数据
excel_df.show()

# 停止SparkSession
spark.stop()