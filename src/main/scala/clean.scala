import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession

object clean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("spark://santa-fe:30251")
      .appName("clean")
      .getOrCreate()

    val trump = getDataFrame(spark, args(0) + "/trump")
    val biden = getDataFrame(spark, args(0) + "/biden")

    val combined = trump.unionAll(biden)
      .groupBy("id","date","user_id","username","place","tweet")
      .agg(collect_list("keyword") as "keyword")
        .withColumn("keyword", col("keyword").cast("String"))

    combined.write.format("csv").option("header", true).save(args(0) + "/clean")
  }

  def getDataFrame(spark: SparkSession, path: String): DataFrame = {
    val keyword = if (path.contains("trump")) "trump" else "biden"

    return spark.read
      .option("header", true)
      .csv(path)
      .filter("language === 'en'")
      .drop(
        "conversation_id",
        "created_at",
        "time",
        "timezone",
        "name",
        "language",
        "mentions",
        "urls",
        "photos",
        "replies_count",
        "retweets_count",
        "likes_count",
        "hashtags",
        "cashtags",
        "link",
        "retweet",
        "quote_url",
        "video",
        "thumbnail",
        "near",
        "geo",
        "source",
        "user_rt_id",
        "user_rt",
        "retweet_id",
        "reply_to",
        "retweet_date",
        "translate",
        "trans_src",
        "trans_dest"
      ).withColumn("keyword", lit(keyword))
  }
}
