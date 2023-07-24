package org.libro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//Ejercicio MnMs
object Chapter2Scala {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()
    /*if (args.length < 1) {
      print("Usage: Chapter2Scala <mnm_file_dataset>")
      sys.exit(1)
    }*/
    // Get the M&M data set filename
    //val mnmFile = args(0)
    // Read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/mnm_dataset.csv")
    // Aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()
    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for California
    caCountMnMDF.show(10)
  }
}

//val mnmFilePath = "src/main/resources/mnm_dataset.csv"

//MnMcount.main(Array(mnmFilePath))
