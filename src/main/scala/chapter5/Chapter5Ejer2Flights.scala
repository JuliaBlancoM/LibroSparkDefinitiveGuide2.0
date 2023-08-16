package chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType.SQL
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object Chapter5Ejer2Flights {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    val delaysPath = "src/main/resources/departuredelays.csv"
    val airportsPath = "src/main/resources/airport-codes-na.txt"

    val airports = spark
      .read
      .options(
        Map(
          "header" -> "true",
          "inferSchema" -> "true",
          "sep" -> "\t"))
      .csv(airportsPath)

    airports.createOrReplaceTempView("airports_na")

    val delays = spark
      .read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))

    delays.createOrReplaceTempView("departureDelays")

    val foo = delays
      .filter(
        expr(
          """
             origin == 'SEA' AND
             destination == 'SFO' AND
             date like '01010%' AND delay > 0
             """))

    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo").show()

    // Union two tables
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")).show()

    //Lo mismo en sql
    spark.sql(
      """
    SELECT *
    FROM bar
    WHERE origin = 'SEA'
       AND destination = 'SFO'
       AND date LIKE '01010%'
       AND delay > 0
    """).show()

    // Join Departure Delays data (foo) with flight info
    foo.join(
      airports.as('air),
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination").show()

    //Lo mismo en SQL
      spark.sql(
        """
    SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
      FROM foo f
      JOIN airports_na a
        ON a.IATA = f.origin
    """).show()

    //Windowing
    //ESTE EJEMPLO NO LO PUEDO HACER PORQUE NO ME DEJA CREAR TABLAS PERO LO HE PROBADO EN DATABRICKS
    val departureDelaysWindow = delays
      .where($"origin".isin("SEA", "SFO", "JFK") &&
        $"destination".isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL"))
      .groupBy("origin", "destination")
      .agg(sum("delay").alias("TotalDelays"))

    departureDelaysWindow.createOrReplaceTempView("departureDelaysWindow")

    val result = spark.table("departureDelaysWindow")
    result.show()

    spark.sql(
      """
    SELECT origin, destination, TotalDelays, rank
    FROM (
    SELECT origin, destination, TotalDelays, dense_rank()
    OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
    FROM departureDelaysWindow
    ) t
    WHERE rank <= 3
    """).show()

    //Modifications

    val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
    foo2.show()

    spark.sql("""SELECT *, CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END AS status FROM foo""").show()

    val foo3 = foo2.drop("delay")
    foo3.show()

    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    //Pivoting

    spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'""").show(10)

    spark.sql(
      """
    SELECT * FROM (
    SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
      FROM departureDelays WHERE origin = 'SEA'
    )
    PIVOT (
      CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
      FOR month IN (1 JAN, 2 FEB, 3 MAR)
    )
    ORDER BY destination
    """).show()

    spark.sql(
      """
    SELECT * FROM (
    SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
      FROM departureDelays WHERE origin = 'SEA'
    )
    PIVOT (
      CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
      FOR month IN (1 JAN, 2 FEB)
    )
    ORDER BY destination
    """).show()

  }

}
