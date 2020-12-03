import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, udf }

object GetLocation {
  val state_codes = List("AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY")

  val state_names = List("alabama",
  "alaska",
  "arizona",
  "arkansas",
  "california",
  "colorado",
  "connecticut",
  "district of columbia",
  "delaware",
  "florida",
  "georgia",
  "hawaii",
  "idaho",
  "illinois",
  "indiana",
  "iowa",
  "kansas",
  "kentucky",
  "louisiana",
  "maine",
  "maryland",
  "massachusetts",
  "michigan",
  "minnesota",
  "mississippi",
  "missouri",
  "montana",
  "nebraska",
  "nevada",
  "new hampshire",
  "new jersey",
  "new mexico",
  "new york",
  "north carolina",
  "north dakota",
  "ohio",
  "oklahoma",
  "oregon",
  "pennsylvania",
  "rhode island",
  "south carolina",
  "south dakota",
  "tennessee",
  "texas",
  "utah",
  "vermont",
  "virginia",
  "washington",
  "west virginia",
  "wisconsin",
  "wyoming")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("spark://santa-fe:30251")
      .appName("Get Location")
      .getOrCreate()

    val st_code: (String => String) = (input: String) => {
      var st = ""
      for (i <- 0 to state_names.length - 1) {
        if (input.toLowerCase.contains(state_names(i)))
          st = state_codes(i)
      }
      for (state <- state_codes) {
        if (input.contains(state))
          st = state
      }
      st
    }

    val st_code_func = udf(st_code)

    val user_list = spark.read
      .option("header", true)
      .csv(args(0))
      .filter("location is not null")
      .distinct
      .drop(
        "name",
        "username",
        "bio",
        "url",
        "join_date",
        "join_time",
        "tweets",
        "following",
        "followers",
        "likes",
        "media",
        "private",
        "verified",
        "profile_image_url",
        "background_image"
      ).withColumn("userId", col("id"))
      .drop("id")
      .withColumn("location", st_code_func(col("location")))
      .filter("location != ''")

    val tweets = spark.read
      .option("header", true)
      .csv(args(1))

    val combined = tweets.join(user_list, tweets("user_id") === user_list("userId"), "inner")
      .drop("place", "userId")

    combined.write.format("csv").option("header", true).save(args(2))
  }
}
