package chapter7

import org.apache.spark.sql.SparkSession


object Chapter7Ejer1Config {

  def printConfigs(session: SparkSession) = {
    // Get conf
    val mconf = session.conf.getAll
    // Print them
    for (k <- mconf.keySet) {
      println(s"${k} -> ${mconf(k)}\n")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .appName("Chapter3Ejer2")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    printConfigs(spark)
    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark)

    val mconf = spark.conf.getAll

    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }

    spark.sql("SET -v").select("key", "value").show(5, false)


  }

}
