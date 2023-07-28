package org.chapter6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType.SQL
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Usage(uid: Int, uname: String, usage: Int)
case class UsageCosts(uid: Int, uname:String, usage: Int, cost: Double)
object Chapter6Ejer2Usage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    import scala.util.Random._

    val r = new scala.util.Random(42)
    // create 1000 instances of scala Usage class
    // this generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))
    // create a dataset of Usage typed data
    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    def filterValue(f: Int) = (v: Int) => v > f

    def filterValue2(v: Int, f: Int): Boolean =  v > f

    def filterWithUsage(u: Usage) = u.usage > 900

    dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

    // use an if-then-else lambda expression and compute a value
    dsUsage.map(u => {
      if (u.usage > 750) u.usage * .15 else u.usage * .50
    }).show(5, false)

    // define a function to compute the usage
    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.50
    }
    // Use the function as an argument to map
    dsUsage.map(u => {
      computeCostUsage(u.usage)
    }).show(5, false)

    def computeUserCostUsage(u: Usage): UsageCosts = {
      val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
      UsageCosts(u.uid, u.uname, u.usage, v)
    }

    dsUsage.map(u => {computeUserCostUsage(u)}).show(5)






  }

}
