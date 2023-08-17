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



# Declare the cubed function
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

# Create the pandas UDF for the cubed function
cubed_udf = pandas_udf(cubed, returnType=LongType())

# Create cubed function using Pandas
x = pd.Series([1, 2, 3])

# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

# Create a Spark DataFrame
df = spark.range(1, 4)

# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()

# Register UDF
spark.udf.register("cubed", cubed)

# Create temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()


# Create an array dataset
arrayData = [[1, (1, 2, 3)], [2, (2, 3, 4)], [3, (3, 4, 5)]]

# Create schema
from pyspark.sql.types import *
arraySchema = (StructType([
      StructField("id", IntegerType(), True),
      StructField("values", ArrayType(IntegerType()), True)
      ]))

# Create DataFrame
df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
df.createOrReplaceTempView("table")
df.printSchema()
df.show()

# Explode and Collect
spark.sql("""
    SELECT id, collect_list(value + 1) AS newValues
    FROM  (SELECT id, explode(values) AS value
           FROM table) x
    GROUP BY id
""").show()


# User Defined Function

# Create UDF
def addOne(values):
  return [value + 1 for value in values]

# Register UDF
spark.udf.register("plusOneIntPy", addOne, ArrayType(IntegerType()))

# Query data
spark.sql("SELECT id, plusOneIntPy(values) AS values FROM table").show()

# Higher order functions
from pyspark.sql.types import *
schema = StructType([StructField("celsius", ArrayType(IntegerType()))])


# Create DataFrame with two rows of two arrays (tempc1, tempc2)
t1 = [35, 36, 32, 30, 40, 42, 38]
t2 = [31, 32, 34, 55, 56]
t_c = spark.createDataFrame([(t1,), (t2,)], ["celsius"])
t_c.createOrReplaceTempView("tC")

# Show the DataFrame
t_c.show()

# Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

# Filter temperatures > 38C for array of temperatures
spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

# Is there a temperature of 38C in the array of temperatures
spark.sql("""
    SELECT celsius, exists(celsius, t -> t = 38) as threshold
    FROM tC
""").show()

# Calculate average temperature and convert to F
spark.sql("""
    SELECT celsius, ((avg(value) * 9) / 5) + 32 as avgFahrenheit
    FROM (
        SELECT celsius, explode(celsius) as value
        FROM tC
    ) x
    GROUP BY celsius
""").show()