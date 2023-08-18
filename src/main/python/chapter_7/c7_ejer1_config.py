from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()

# view only the Spark SQL–specific Spark configs

spark.sql("SET -v").select("key", "value").show(n=5, truncate=False)

# comprobar si es modificable y modificar una configuración
print(spark.conf.isModifiable("spark.sql.shuffle.partitions"))
spark.conf.get("spark.sql.shuffle.partitions")
'200'

#PRUEBA EJER CAP 8
# Step 1: define input sources
lines = (spark
.readStream.format("socket")
.option("host", "localhost")
.option("port", 9999)
.load())

# Step 2: transform data
words = lines.select(split(col("value"), "\\s").alias("word"))
counts = words.groupBy("word").count()

checkpointDir = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/checkpoint"
streamingQuery = (counts
                  .writeStream
                  .format("console")
                  .outputMode("complete")
                  .trigger(processingTime="1 second")
                  .option("checkpointLocation", checkpointDir)
                  .start())
streamingQuery.awaitTermination()

# Step 3: Define Output Sink and Output Mode

# writer = counts.writeStream.format("console").outputMode("complete")

# Step4: Specify processing details
'''
checkpointDir = "C:/Users/julia.blanco/Desktop/repositorios/LibroSpark/src/main/resources/checkpoint"
writer2 = (writer
.trigger(processingTime="1 second")
.option("checkpointLocation", checkpointDir))
'''
# Step5: start the query
# streamingQuery = writer2.start()