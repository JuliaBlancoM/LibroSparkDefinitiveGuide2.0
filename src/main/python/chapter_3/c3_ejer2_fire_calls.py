from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()

sf_fire_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/sf-fire-calls.csv"

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),
                     StructField('CallDate', StringType(), True),
                     StructField('WatchDate', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),
                     StructField('City', StringType(), True),
                     StructField('Zipcode', IntegerType(), True),
                     StructField('Battalion', StringType(), True),
                     StructField('StationArea', StringType(), True),
                     StructField('Box', StringType(), True),
                     StructField('OriginalPriority', StringType(), True),
                     StructField('Priority', StringType(), True),
                     StructField('FinalPriority', IntegerType(), True),
                     StructField('ALSUnit', BooleanType(), True),
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('Delay', FloatType(), True)])

fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
fire_df.cache()
print(fire_df.count())
fire_df.printSchema()
fire_df.show(n=10, truncate=False)

#Filter out "Medical Incident" call types
few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))

few_fire_df.show(5, truncate=False)
#How many distinct types of calls were made to the Fire Department?
count_calls = fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().count()
print(count_calls)

#What distinct types of calls were made to the Fire Department?
fire_df.select("CallType").where(col("CallType").isNotNull()).distinct().show(10, False)

#Find out all response or delayed times greater than 5 mins?
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
new_fire_df.select("ResponseDelayedinMins").where(col("ResponseDelayedinMins") > 5).show(5, False)

fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate")
              .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")).drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm"))
fire_ts_df.cache()
fire_ts_df.columns
fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)

#What were the most common call types?
(fire_ts_df
 .select("CallType").where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))

#What zip codes accounted for most common calls?
(fire_ts_df
 .select("CallType", "ZipCode")
 .where(col("CallType").isNotNull())
 .groupBy("CallType", "Zipcode")
 .count()
 .orderBy("count", ascending=False)
 .show(10, truncate=False))

#What San Francisco neighborhoods are in the zip codes 94102 and 94103
fire_ts_df.select("Neighborhood", "Zipcode").where((col("Zipcode") == 94102) | (col("Zipcode") == 94103)).distinct().show(10, truncate=False)

#What was the sum of all calls, average, min and max of the response times for calls?
fire_ts_df.select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"), max("ResponseDelayedinMins")).show()

#How many distinct years of data is in the CSV file?
fire_ts_df.select(year("IncidentDate")).distinct().orderBy(year("IncidentDate")).show()

#What week of the year in 2018 had the most fire calls?
fire_ts_df.filter(year("IncidentDate") == 2018).groupBy(weekofyear("IncidentDate")).count().orderBy("count", ascending=False).show()

#What neighborhoods in San Francisco had the worst response time in 2018?
fire_ts_df.select("Neighborhood", "ResponseDelayedinMins").filter(year("IncidentDate") == 2018).show(10, False)

#How can we use Parquet files or SQL table to store data and read it back?
fire_ts_df.write.format("parquet").mode("overwrite").save("C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/fire_calls_parquet/")

#How can we use Parquet SQL table to store data and read it back?
#fire_ts_df.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls")

#How can we read data from Parquet file?
file_parquet_df = spark.read.format("parquet").load("C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/fire_calls_parquet/")

file_parquet_df.show(n=9, truncate=False)