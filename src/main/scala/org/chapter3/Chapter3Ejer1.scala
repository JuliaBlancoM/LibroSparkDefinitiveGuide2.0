package org.chapter3

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
object Chapter3Ejer1 {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("AuthorsAges")
      .master("local[2]")
      .getOrCreate()

    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

    val avgDF: DataFrame = dataDF.groupBy("name").agg(avg("age"))

    avgDF.show()

  }

}
