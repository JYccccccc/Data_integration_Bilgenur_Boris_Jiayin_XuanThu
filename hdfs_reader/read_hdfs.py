from pyspark.sql import SparkSession
# creer SparkSession
spark = SparkSession.builder \
    .appName("HDFSDataLoader") \
    .getOrCreate()

# Lire les données depuis HDFS
methods_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .load("hdfs:///database/Methods_2022_8_1.xlsx")

site_info_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .load("hdfs:///database/Site_Information_2022_8_1.xlsx")

# afficher les données
methods_df.show()
site_info_df.show()
