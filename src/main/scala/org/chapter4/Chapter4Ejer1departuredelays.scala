package org.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Chapter4Ejer1departuredelays {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val csvFile="src/main/resources/departuredelays.csv"

    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql("""SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""")
      .show(10)

    //Convertido a DataFrame API:
    df.select("distance","origin", "destination")
      .where(col("distance") > 1000).orderBy(desc("distance"))
      .show(10)

    spark.sql("""SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""")
      .show(10)

    //convertido a DataFrame API:
    df
      .filter(col("delay") > 120 && col("origin") === "SFO" && col("destination") === "ORD")
      .select("date", "delay", "origin", "destination")
      .orderBy(col("delay").desc)
      .show(10)

    spark.sql("""SELECT delay, origin, destination,
    CASE
    WHEN delay > 360 THEN 'Very Long Delays'
    WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
    WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
    WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
    WHEN delay = 0 THEN 'No Delays'
    ELSE 'Early'
    END AS Flight_Delays
    FROM us_delay_flights_tbl
    ORDER BY origin, delay DESC""")
      .show(10)

    //Convertido a DataFrame API:
    df.withColumn("Flight_Delays",
        when(col("delay") > 360, "Very Long Delays")
          .when(col("delay") > 120 && col("delay") < 360, "Long Delays")
          .when(col("delay") > 60 && col("delay") < 120, "Short Delays")
          .when(col("delay") > 0 && col("delay") < 60, "Tolerable Delays")
          .when(col("delay") === 0, "No Delays")
          .otherwise("Early")
      )
      .select("delay", "origin", "destination", "Flight_Delays")
      .orderBy(col("origin"), col("delay").desc)
      .show(10)

    //Creating Views:

    df.createOrReplaceTempView("us_delay_flights_tbl")

    val df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
    val df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")






  }

}
