from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[*]") \
    .getOrCreate()

mnm_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/mnm_dataset.csv"

mnm_df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

count_mnm_df = (mnm_df
                .select("State", "Color", "Count")
                .groupBy("State", "Color")
                .agg(count("Count").alias("Total"))
                .orderBy("Total", ascending=False))

count_mnm_df.show(n=60, truncate=False)
print(f"Total Rows = {count_mnm_df.count()}")

ca_count_mnm_df = (mnm_df
                   .select("State", "Color", "Count")
                   .where(mnm_df.State == "CA")
                   .groupBy("State", "Color")
                   .agg(count("Count").alias("Total"))
                   .orderBy("Total", ascending=False))

ca_count_mnm_df.show(n=10, truncate=False)