package sql

import utils.SparkHelper._


object SelectWithWhere {
  def main(args: Array[String]): Unit = {

    val spark = createSparkSession(appName = "SelectWithWhere", master = "local[*]")

    val df = spark.sql("SELECT * FROM spark_db.users WHERE dt = '20241108' AND age > 30 ORDER BY name, age")
    df.explain(true)

  }

}
