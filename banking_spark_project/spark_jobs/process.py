from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from collections import Counter
import pandas as pd

DB_URL = "jdbc:postgresql://postgres:5432/airflow"
DB_PROPERTIES = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

def main():
    spark = SparkSession.builder.appName("BankTransactionsML").getOrCreate()

    # Чтение
    df = spark.read.parquet("hdfs://namenode:9000/user/data/transactions.parquet")

    # UDF
    def get_most_common(seq):
        if not seq: return '5411'
        try: return Counter(str(seq).split()).most_common(1)[0][0]
        except: return '5411'

    most_common_udf = udf(get_most_common, StringType())

    df_transformed = df.select(
        col("client_pin_hash"), col("age"), col("adminarea"),
        most_common_udf(col("prev_mcc_seq")).alias("fave_mcc_code")
    )

    # Mapping
    try:
        pdf_mapping = pd.read_excel("/opt/airflow/data/mcc_to_cat_mapping.xlsx", dtype=str)
        spark_mapping = spark.createDataFrame(pdf_mapping)
        final_df = df_transformed.join(spark_mapping, df_transformed.fave_mcc_code == spark_mapping.mcc, "left").drop("mcc")
    except:
        final_df = df_transformed.withColumn("eng_cat", col("fave_mcc_code"))

    # Агрегация
    region_stats = df.groupBy("adminarea").count().withColumnRenamed("count", "client_count")

    # Запись
    final_df.limit(5000).write.jdbc(url=DB_URL, table="client_predictions_spark", mode="overwrite", properties=DB_PROPERTIES)
    region_stats.write.jdbc(url=DB_URL, table="region_stats_spark", mode="overwrite", properties=DB_PROPERTIES)

    spark.stop()

if __name__ == "__main__":
    main()