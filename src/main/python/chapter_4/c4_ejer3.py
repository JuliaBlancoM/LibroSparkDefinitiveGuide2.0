from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.catalog import Catalog

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()

us_flights_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/flights/departuredelays.csv"

"""
spark.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)")

databases = spark.catalog.listDatabases()
for database in databases:
    print(database.name)
"""
df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", us_flights_file)
      .load())

df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")




