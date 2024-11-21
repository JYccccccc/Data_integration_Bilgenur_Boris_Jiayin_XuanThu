from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("ReadExcelFromHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()


excel_file_path = "hdfs://namenode:8020/data/Site_Information_2022_8_1.xlsx"


excel_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Sheet 1'!A1") \
    .load(excel_file_path)


excel_df.show()


spark.stop()