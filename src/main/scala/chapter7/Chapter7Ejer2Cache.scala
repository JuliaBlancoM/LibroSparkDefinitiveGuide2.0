package chapter7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Chapter7Ejer2Cache {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    df.cache() // Cache the data

    val count = df.count() // Materialize the cache
    println(count)

    df.createOrReplaceTempView("dfTable")
    spark.sql("CACHE TABLE dfTable")
    spark.sql("SELECT count(*) FROM dfTable").show()


  }
}
