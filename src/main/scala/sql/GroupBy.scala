package sql

object GroupBy {
  def main(args: Array[String]): Unit = {
    val spark = utils.SparkHelper.createSparkSession(appName = "GroupBy", master = "local[*]")
    spark.sql("USE spark_db")
    spark.sql("CREATE TABLE IF NOT EXISTS spark_db.sales (product STRING, category STRING, amount INT) USING ORC")
    spark.sql("""INSERT OVERWRITE spark_db.sales VALUES
        |('Laptop', 'Electronics', 1200),
        |('Smartphone', 'Electronics', 800),
        |('Tablet', 'Electronics', 600),
        |('Headphones', 'Electronics', 150),
        |('Refrigerator', 'Appliances', 2000),
        |('Washing Machine', 'Appliances', 1500),
        |('Microwave', 'Appliances', 300),
        |('Blender', 'Appliances', 100),
        |('Sofa', 'Furniture', 700),
        |('Dining Table', 'Furniture', 1200),
        |('Chair', 'Furniture', 150),
        |('Bed', 'Furniture', 1000),
        |('T-Shirt', 'Clothing', 20),
        |('Jeans', 'Clothing', 50),
        |('Jacket', 'Clothing', 100),
        |('Sneakers', 'Clothing', 80)""".stripMargin)


    val result = spark.sql("SELECT category, SUM(amount) AS total_amount FROM spark_db.sales GROUP BY category")
    result.explain(true)
    result.show(numRows = 20, truncate = false)

    Thread.sleep(60 * 1000 * 5) // Sleep for 5 minutes so that we can check the Spark UI
    // Stop the Spark session
    spark.stop()
  }

}
