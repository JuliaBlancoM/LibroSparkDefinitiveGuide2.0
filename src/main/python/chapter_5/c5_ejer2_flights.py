from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()

# paths
delaysPath = "src/main/resources/departuredelays.csv"
airportsPath = "src/main/resources/airport-codes-na.txt"

# Read airports data
airports = spark.read.options(header=True, inferSchema=True, sep="\t").csv(airportsPath)
airports.createOrReplaceTempView("airports_na")

# Read delays data
delays = (spark.read.option("header", "true").csv(delaysPath)
          .withColumn("delay", expr("CAST(delay as INT) as delay"))
          .withColumn("distance", expr("CAST(distance as INT) as distance")))
delays.createOrReplaceTempView("departureDelays")

# Filter delays data
foo = delays.filter(expr("""
     origin == 'SEA' AND
     destination == 'SFO' AND
     date like '01010%' AND delay > 0
     """))
foo.createOrReplaceTempView("foo")

# Display
spark.sql("SELECT * FROM airports_na LIMIT 10").show()
spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
spark.sql("SELECT * FROM foo").show()

# Union
bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")).show()

# Equivalent SQL operation
spark.sql("""
SELECT *
FROM bar
WHERE origin = 'SEA'
   AND destination = 'SFO'
   AND date LIKE '01010%'
   AND delay > 0
""").show()

# Join
(foo.join(airports.alias("air"), expr("air.IATA = origin"))
     .select("City", "State", "date", "delay", "distance", "destination").show())

# Equivalent SQL operation
spark.sql("""
SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
  FROM foo f
  JOIN airports_na a
    ON a.IATA = f.origin
""").show()

# Omitted windowing example for brevity. You'd use Window functions in PySpark.

# Modifications
foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
foo2.show()

spark.sql("""SELECT *, CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END AS status FROM foo""").show()

foo3 = foo2.drop("delay")
foo3.show()

foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# Pivoting (similar to Scala; slight differences in syntax)
spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'""").show(10)

# Pivoting examples
spark.sql("""
SELECT * FROM (
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
  FROM departureDelays WHERE origin = 'SEA'
)
PIVOT (
  CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
  FOR month IN (1 JAN, 2 FEB, 3 MAR)
)
ORDER BY destination
""").show()

spark.sql("""
SELECT * FROM (
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
  FROM departureDelays WHERE origin = 'SEA'
)
PIVOT (
  CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
  FOR month IN (1 JAN, 2 FEB)
)
ORDER BY destination
""").show()