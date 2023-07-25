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
      StructField("WatchDate", StringType, true),
      StructField("CallType", StringType, true),
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
    val sfFireFile = "src/main/resources/Fire_Incidents.csv"
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)

    /*val parquetPath = "src/main/resources/Fire_Incidents.parquet"
    fireDF.write.format("parquet").save(parquetPath)*/

    /*val parquetTable = "Fire_Incidents_Table"
    fireDF.write.format("parquet").saveAsTable(parquetTable)*/
    val fewFireDF = fireDF
      //.select("IncidentNumber", "AvailableDtTm", "Address")
      //.where($"CallType" =!= "Medical Incident")
    fewFireDF.show(5, false)

  }

}
