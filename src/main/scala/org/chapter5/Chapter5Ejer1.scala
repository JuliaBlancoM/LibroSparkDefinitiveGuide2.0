package org.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Chapter5Ejer1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    // Create cubed function
    val cubed = (s: Long) => {
      s * s * s
    }

    // Register UDF
    spark.udf.register("cubed", cubed)

    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    //Load and save to a PostgreSQL database using the Spark SQL datasource API and JDBC in Scala:
    /*val jdbcDF1 = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:[DBSERVER]")
      .option("dbtable", "[SCHEMA].[TABLENAME]")
      .option("user", "USUARIO")
      .option("password", "PASSWORD")
      .load()*/

    import org.apache.spark.sql.Row
    // Create an array dataset
    val arrayData = Seq(
      Row(1, List(1, 2, 3)),
      Row(2, List(2, 3, 4)),
      Row(3, List(3, 4, 5))
    )

    // Create schema
    val arraySchema = new StructType()
      .add("id", IntegerType)
      .add("values", ArrayType(IntegerType))

    // Create DataFrame
    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
    df.createOrReplaceTempView("table")
    df.printSchema()
    df.show()

    //Explode and Collect
    spark.sql(
      """
    SELECT id, collect_list(value + 1) AS newValues
      FROM  (SELECT id, explode(values) AS value
            FROM table) x
     GROUP BY id
    """).show()

    // User Defined Function
    // Create UDF
    def addOne(values: Seq[Int]): Seq[Int] = {
      values.map(value => value + 1)
    }

    // Register UDF
    val plusOneInt = spark.udf.register("plusOneInt", addOne(_: Seq[Int]): Seq[Int])

    // Query data
    spark.sql("SELECT id, plusOneInt(values) AS values FROM table").show()

    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val tC = Seq(t1, t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")

    // Show the DataFrame
    tC.show()

    // Calculate Fahrenheit from Celsius for an array of temperatures
    spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

    // Filter temperatures > 38C for array of temperatures
    spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

    // Is there a temperature of 38C in the array of temperatures
    spark.sql(
      """
    SELECT celsius, exists(celsius, t -> t = 38) as threshold
    FROM tC
    """).show()


    // Calculate average temperature and convert to F
    //La función reduce me da error: Exception in thread "main":
    // Undefined function: reduce. This function is neither a built-in/temporary function,
    // nor a persistent function that is qualified as spark_catalog.

    spark.sql(
      """
    SELECT celsius, ((avg(value) * 9) / 5) + 32 as avgFahrenheit
    FROM (
        SELECT celsius, explode(celsius) as value
        FROM tC
    ) x
    GROUP BY celsius
    """).show()

    //ERROR REDUCE
    /*

    spark.sql(
      """
    SELECT celsius,
           reduce(
              celsius,
              0,
              (t, acc) -> t + acc,
              acc -> (acc div size(celsius) * 9 div 5) + 32
            ) as avgFahrenheit
      FROM tC
    """).show()

    */






  }

}
