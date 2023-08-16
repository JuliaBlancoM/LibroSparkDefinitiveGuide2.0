package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
object Chapter3Ejer2Blogs {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    if (args.length <= 0) {
      println("usage Chapter3Ejer2 <src/main/resources/blogs.json>")
      System.exit(1)
    }
    // Get the path to the JSON file
    val jsonFile = args(0)
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)
    // Print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)
    blogsDF.select(expr("Hits * 2")).show(2)
    blogsDF.select(col("Hits") * 2).show(2)
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
    blogsDF
      .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
    blogsDF
      .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)
    /* These statements return the same value, showing that
    expr is the same as a col method call */
    blogsDF.select(expr("Hits")).show(2)
    blogsDF.select(col("Hits")).show(2)
    blogsDF.select("Hits").show(2)
    // Sort by column "Id" in descending order
    blogsDF.sort(col("Id").desc).show()
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))
    // Access using index for individual items
    println(blogRow(1))
    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val authorsDF = rows.toDF("Author", "State")
    authorsDF.show()
  }
}

