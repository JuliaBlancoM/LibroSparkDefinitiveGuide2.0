package org.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class DeviceIoTData(battery_level: Long, c02_level: Long,
                         cca2: String, cca3: String, cn: String, device_id: Long,
                         device_name: String, humidity: Long, ip: String, latitude: Double,
                         lcd: String, longitude: Double, scale: String, temp: Long, timestamp: Long)

case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long, cca3: String)

object Chapter3Ejer4IoT {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val ds = spark.read
      .json("src/main/resources/iot_devices.json")
      .as[DeviceIoTData]

    ds.show(5, false)


    val filterTempDS = ds.filter(d => d.temp > 30 && d.humidity > 70)

    filterTempDS.show(5, false)

    val dsTemp = ds
      .filter(d => {
        d.temp > 25
      }).map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .withColumnRenamed("_1", "temp")
      .withColumnRenamed("_2", "device_name")
      .withColumnRenamed("_3", "device_id")
      .withColumnRenamed("_4", "cca3").as[DeviceTempByCountry]

    dsTemp.show(10)

    val dsTemp2 = ds.select($"temp", $"device_name", $"device_id", $"device_id", $"cca3").where("temp > 25").as[DeviceTempByCountry]

    dsTemp2.show(5, false)

    //End-to-End Dataset Example:
    //1. Detect failing devices with battery levels below a threshold.

    ds.select($"battery_level", $"c02_level", $"device_name")
      .where($"battery_level" < 8)
      .sort($"c02_level")
      .show(5, false)

    //Otra forma de hacerlo:

    val filterTempDS3 = ds.filter(d => d.battery_level < 8)

    filterTempDS3.select($"battery_level", $"c02_level", $"device_name")
      .sort($"c02_level")
      .show(5, false)

    //2. Identify offending countries with high levels of CO2 emissions.

    val newDS = ds
      .filter(d => {
        d.c02_level > 1300
      })
      .groupBy($"cn")
      .avg()
      .sort($"avg(c02_level)".desc)

    newDS.show(10, false)

    //3. Compute the min and max values for temperature, battery level, CO2, and
    //humidity.

    ds.select(min("temp"), max("temp"),
      min("battery_level"), max("battery_level"),
      min("humidity"), max("humidity"),
      min("c02_level"), max("c02_level"),
      min("battery_level"), max("battery_level"))
      .show(10)

    //4. Sort and group by average temperature, CO2, humidity, and country.

    ds.filter(d => {
      d.temp > 25 && d.humidity > 75
    })
      .select("temp", "humidity", "cn")
      .groupBy($"cn")
      .avg()
      .sort($"avg(temp)".desc, $"avg(humidity)".desc).as("avg_humidity").show(10, false)







  }

}
