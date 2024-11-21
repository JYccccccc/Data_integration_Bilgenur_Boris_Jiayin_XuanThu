from pyspark.sql import DataFrame

def process_data(kafka_data: list, methods_df: DataFrame, site_info_df: DataFrame):
    # 这里可以定义如何将Kafka的数据与HDFS中的数据进行集成
    # 比如将Kafka数据转换成DataFrame后与methods_df和site_info_df进行合并
    print("Processing data...")
    # 示例代码
    for data in kafka_data:
        print(f"Processing {data}")
    return "Integrated dataset"  # 返回集成后的数据集
