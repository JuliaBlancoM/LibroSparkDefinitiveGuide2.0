package chapter4

import org.apache.hadoop.shaded.org.jline.keymap.KeyMap.display
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Chapter4Ejer2departuredelays {

  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .config("spark.sql.warehouse.dir", "C:\\spark-warehouse")
      .appName("DataFrame comparator")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._



    //El resto lo dejo comentado porque no tengo un HADOOP_HOME


    val df = spark
      .read
      .format("csv")
      .schema("`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING")
      .option("header", "true")
      .option("path", "src/main/resources/departuredelays.csv")
      .load()


    //df.write.saveAsTable("delays")

   /*spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    df.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("/tmp/data/parquet/df_parquet")*/
      



    //A partir de aquí lo dejo comentado porque al no tener un HADOOP_HOME set no puedo hacerlo


    //Create Managed Table:
    spark.sql("CREATE TABLE me_invento_el_nombre (date STRING, delay INT, distance INT, origin STRING, destination STRING)")
    //Hacer lo mismo con la DataFrame API:
    // Path to our US flight delays CSV file
    val csv_file = "src/main/resources/departuredelays.csv"
    // Schema as defined in the preceding example
    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val flights_df = spark.read.format("csv").schema(schema).load(csv_file)
    flights_df.write.saveAsTable("me_invento_el_nombre")

    //Create Unmanaged Table:
    /*spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)
    USING csv OPTIONS (PATH 'src/main/resources/departuredelays.csv')""")*/
    //Lo mismo con la DataFrame API:
    /*(flights_df
    .write
    .option("path", "/tmp/data/us_flights_delay")
    .saveAsTable("us_delay_flights_tbl"))*/

    /*val df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
    val df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")*/

    // Create a temporary and global temporary view
    /*df_sfo.createGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

    spark.catalog.listDatabases()
    spark.catalog.listTables()
    spark.catalog.listColumns("us_delay_flights_tbl")

    val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
    val usFlightsDF2 = spark.table("us_delay_flights_tbl")*/

    // In Scala
    // Use Parquet
    /*
    val file = """src/main/resources/flights/summarydata/
    parquet/2010-summary.parquet"""
    val df = spark.read.format("parquet").load(file)*/
    // Use Parquet; you can omit format("parquet") if you wish as it's the default
    //val df2 = spark.read.load(file) */


    // Use CSV


    //val df3 = spark.read.format("csv")
      //.option("inferSchema", "true")
      //.option("header", "true")
      //.option("mode", "PERMISSIVE")
      //.load("src/main/resources/flights/summary-data/csv/*")


    // Use JSON
    //val df4 = spark.read.format("json")
    //.load("src/main/resources/flights/summary-data/json/*")


  }

}
