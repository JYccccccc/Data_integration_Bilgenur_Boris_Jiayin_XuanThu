from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Creer SparkSession
spark = SparkSession.builder \
    .appName("MetricsCalculator") \
    .getOrCreate()

# Lire les données intégrées
integrated_df = spark.read \
    .format("parquet") \
    .load("hdfs://namenode:8020/output/integrated_data")

# Calculer les tendances des métriques
ph_trend = integrated_df.groupBy("Site_ID") \
    .agg(avg("pH").alias("average_ph")) \
    .orderBy("Site_ID")

sulfur_trend = integrated_df.groupBy("Site_ID") \
    .agg(avg("Sulphate").alias("average_sulphate")) \
    .orderBy("Site_ID")

# Sauvegarder les tendances des métriques
ph_trend.write.format("parquet").mode("overwrite").save("hdfs://namenode:8020/data/metrics/ph_trend")
sulfur_trend.write.format("parquet").mode("overwrite").save("hdfs://namenode:8020/data/metrics/sulfur_trend")
