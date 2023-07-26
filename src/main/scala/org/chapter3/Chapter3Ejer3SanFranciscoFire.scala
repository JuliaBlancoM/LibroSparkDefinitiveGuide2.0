package org.chapter3

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
    // Select the converted columns
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

  }

}

