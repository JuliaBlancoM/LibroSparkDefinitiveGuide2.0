package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Chapter3Ejer3SanFranciscoFire {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
      StructField("UnitID", StringType, true),
      StructField("IncidentNumber", IntegerType, true),
      StructField("CallType", StringType, true),
      StructField("CallDate", StringType, true),
      StructField("WatchDateCallType", StringType, true),
      StructField("CallFinalDisposition", StringType, true),
      StructField("AvailableDtTm", StringType,true),
      StructField("Address", StringType,true),
      StructField("City", StringType,true),
      StructField("Zipcode", IntegerType,true),
      StructField("Battalion", StringType,true),
      StructField("StationArea", StringType,true),
      StructField("Box", StringType,true),
      StructField("OriginalPriority", StringType,true),
      StructField("Priority", StringType,true),
      StructField("FinalPriority", IntegerType,true),
      StructField("ALSUnit", BooleanType,true),
      StructField("CallTypeGroup", StringType,true),
      StructField("NumAlarms", IntegerType,true),
      StructField("UnitType", StringType,true),
      StructField("UnitSequenceInCallDispatch", IntegerType,true),
      StructField("FirePreventionDistrict", StringType,true),
      StructField("SupervisorDistrict", StringType,true),
      StructField("Neighborhood", StringType,true),
      StructField("Location", StringType,true),
      StructField("RowID", StringType,true),
      StructField("Delay", FloatType,true)))
    // Read the file using the CSV DataFrameReader
    val sfFireFile = "src/main/resources/sf-fire-calls.csv"
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    /*val parquetPath = "src/main/resources/Fire_Incidents.parquet"
    fireDF.write.format("parquet").save(parquetPath)*/

    /*val parquetTable = "Fire_Incidents_Table"
    fireDF.write.format("parquet").saveAsTable(parquetTable)*/

    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where($"CallType" =!= "Medical Incident")
    fewFireDF.show(5, false)

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
      .show()
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show(10, false)

    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where($"ResponseDelayedinMins" > 5)
      .show(5, false)

    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDateCallType"), "MM/dd/yyyy"))
      .drop("WatchDateCallType")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    fireTsDF.cache()
    fireTsDF.columns

    // Select the converted columns
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    fireTsDF
      .select(year($"IncidentDate"))
      .distinct()
      .orderBy(year($"IncidentDate"))
      .show()

    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    import org.apache.spark.sql.{functions => F}
    fireTsDF
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
      .show()

    // Questions from page 68
    // What were all the different types of fire calls in 2018?
    fireTsDF
      .filter(year($"IncidentDate") === 2018)
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show()
    // What months within the year 2018 saw the highest number of fire calls?
    fireTsDF
      .filter(year($"IncidentDate") === 2018)
      .groupBy(month($"IncidentDate"))
      .count()
      .orderBy(desc("count"))
      .show()

    // Which neighborhood in San Francisco generated the most fire calls in 2018?
    fireTsDF
      .filter(year($"IncidentDate") === 2018)
      .groupBy("Neighborhood")
      .count()
      .orderBy(desc("count"))
      .show(1)

    // Which neighborhoods had the worst response times to fire calls in 2018?
    fireTsDF
      .select("Neighborhood", "ResponseDelayedinMins")
      .filter(year($"IncidentDate") === 2018)
      .orderBy(desc("ResponseDelayedinMins"))
      .show()
    // Which week in the year in 2018 had the most fire calls?
    fireTsDF
      .filter(year($"IncidentDate") === 2018)
      .groupBy(weekofyear($"IncidentDate"))
      .count()
      .orderBy(desc("count"))
      .show()
    // Is there a correlation between neighborhood, zip code, and number of fire calls?
    fireTsDF
      .select("Neighborhood", "ZipCode")
      .groupBy("Neighborhood", "Zipcode")
      .count()
      .orderBy(desc("count"))
      .show(10, false)
    //How can we use Parquet files or SQL tables to store this data and read it back?
    /*
    fireTsDF
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/src/main/resources/fireServiceParquet/")

      %fs ls /tmp/fireServiceParquet/
      */


  }

}

