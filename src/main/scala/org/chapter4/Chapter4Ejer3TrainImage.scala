package org.chapter4

import org.apache.hadoop.shaded.org.jline.keymap.KeyMap.display
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Chapter4Ejer3TrainImage {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._


    println(spark.version)

    val imageDir = "src/main/resources/train_images/"
    val imagesDF = spark.read.format("image").load(imageDir)








  }

}
