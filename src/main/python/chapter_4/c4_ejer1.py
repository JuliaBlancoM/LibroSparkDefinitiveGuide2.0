from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()

def to_date_format_udf(d_str):
  l = [char for char in d_str]
  return "".join(l[0:2]) + "/" +  "".join(l[2:4]) + " " + " " +"".join(l[4:6]) + ":" + "".join(l[6:])

to_date_format_udf("02190925")

spark.udf.register("to_date_format_udf", to_date_format_udf, StringType())

us_delay_flights_df = (spark.read.format("csv")
      .schema("date STRING, delay INT, distance INT, origin STRING, destination STRING")
      .option("header", "true")
      .option("path", "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/flights/departuredelays.csv")
      .load())

us_delay_flights_df.show(n=10, truncate=False)

us_delay_flights_df.selectExpr("to_date_format_udf(date) as data_format").show(10, truncate=False)

us_delay_flights_df.createOrReplaceTempView("us_delay_flights_tbl")

us_delay_flights_df.withColumn("date_fm", expr("to_date_format_udf(date)")).show(10, truncate=False)
print(us_delay_flights_df.count())

#Find out all flights whose distance between origin and destination is greater than 1000
us_delay_flights_df.select("distance", "origin", "destination") \
    .filter(col("distance") > 1000) \
    .orderBy(desc("distance")) \
    .show(10, truncate=False)

#Find out all flights with 2 hour delays between San Francisco and Chicago
us_delay_flights_df.select("date", "delay", "origin", "destination") \
    .filter((col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD")) \
    .orderBy(desc("delay")) \
    .show(10, truncate=False)

#Let's label all US flights originating from airports with high, medium, low, no delays, regardless of destinations.
us_delay_flights_df.select(
    "delay", "origin", "destination",
    when(col("delay") > 360, "Very Long Delays")
    .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
    .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
    .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
    .when(col("delay") == 0, "No Delays")
    .otherwise("No Delays").alias("Flight_Delays")
).orderBy("origin", desc("delay")).show(10, truncate=False)

df1 = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view")

global_temp_df = spark.table("global_temp.us_origin_airport_SFO_tmp_view")
global_temp_df.show()

spark.catalog.dropGlobalTempView("us_origin_airport_JFK_tmp_view")

df2 = us_delay_flights_df.select("date", "delay", "origin", "destination").filter(col("origin") == "JFK")
df2.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

spark.catalog.listTables(dbName="global_temp")
