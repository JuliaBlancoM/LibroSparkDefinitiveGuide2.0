from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()

parquet_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/flights/summary-data/parquet/2010-summary.parquet"
json_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/flights/summary-data/json/*"
csv_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/flights/summary-data/csv/*"
orc_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/flights/summary-data/orc/*"
avro_file = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/flights/summary-data/avro/*"
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

df = spark.read.format("parquet").option("path", parquet_file).load()

df.show(10, False)

df = spark.read.format("json").option("path", json_file).load()

df.show(10, False)

df.createOrReplaceTempView("us_delay_flights_tbl")

df.show(9, False)

df = (spark
      .read
	 .format("csv")
	 .option("header", "true")
	 .schema(schema)
	 .option("mode", "FAILFAST")  # exit if any errors
	 .option("nullValue", "")	  # replace any null data field with “”
	 .option("path", csv_file)
	 .load())

df.show(10, truncate = False)

df.write.format("parquet")\
    .mode("overwrite")\
    .option("path", "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/tmp/data/parquet/df_parquet")\
    .option("compression", "snappy")\
    .save()

df.createOrReplaceTempView("us_delay_flights_tbl")
temp_df = spark.table("us_delay_flights_tbl")
temp_df.show(8, truncate=False)

df = (spark.read
      .format("orc")
      .option("path", orc_file)
      .load())
df.show(10, truncate=False)

#df = (spark.read
#      .format("avro")
#      .option("path", avro_file)
#      .load())
#df.show(10, truncate=False)

from pyspark.ml import image

image_dir = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

images_df.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, truncate=False)

path = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/train_images/"
binary_files_df = (spark
                   .read
                   .format("binaryFile")
                   .option("pathGlobFilter", "*.jpg")
                   .option("recursiveFileLookup", "true")
                   .load(path))
binary_files_df.show(5)