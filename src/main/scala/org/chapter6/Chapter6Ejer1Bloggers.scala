package org.chapter6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType.SQL
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Bloggers(id:Int, first:String, last:String, url:String, date:String,
                    hits: Int, campaigns:Array[String])
object Chapter6Ejer1Blogger {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val bloggers = "src/main/resources/blogs.json"

    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]










  }
}
