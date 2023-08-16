package chapter4

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


    /*println(spark.version)

    val imageDir = "src/main/resources/train_images/"
    val imagesDF = spark.read.format("image").load(imageDir)

    imagesDF.printSchema
    imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode",
      "label").show(5, false)

    */
    //Con la lectura de Binary Files también me encuentro problemas relacionados con incompatibilidad entre Windows y Hadoop así que lo dejo comentado
    /*
    val path = "src/main/resources/train_images/"
    val binaryFilesDF = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .load(path)
    binaryFilesDF.show(5)
    */

  }

}
