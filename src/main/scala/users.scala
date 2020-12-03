import org.apache.spark.sql.SparkSession

object users {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("spark://santa-fe:30251")
      .appName("clean")
      .getOrCreate()

    val input = spark.read
      .option("header", true)
      .csv(args(0) + "/clean")
      .drop("id","date","username","place","tweet","keyword")
      .distinct

    input.write.format("csv").save(args(0) + "/users")
  }
}
