package utils

object SparkHelper {

  val warehouseLocation: String = new java.io.File("spark-warehouse").getAbsolutePath
  def createSparkSession(appName: String, master: String): org.apache.spark.sql.SparkSession = {
    val log4jConf =
      "-Dlog4j.configurationFile=/Users/tan.vu/github/LearningSparkV2/chapter2/scala/src/main/resources/log4j2.properties"

    val addOpens =
      "--add-opens=java.base/java.net=ALL-UNNAMED"

    val driverOpts   = s"$addOpens $log4jConf"
    val executorOpts = s"$addOpens $log4jConf"

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName(appName)
      .master(master)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.driver.extraJavaOptions", driverOpts)
      .config("spark.executor.extraJavaOptions", executorOpts)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

}
