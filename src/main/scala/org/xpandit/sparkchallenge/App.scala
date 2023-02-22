package org.xpandit.sparkchallenge

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

/**
 * @author ${user.name}
 */
object App {
  def part1(dataframe: DataFrame): DataFrame = {
    // Compute the average sentiment polarity (and give the respective column a new name), grouped by app
    val df_1 = dataframe
      .groupBy("App")
      .agg(avg("Sentiment_Polarity")
        .alias("Average_Sentiment_Polarity"))
      .na.fill(0.0, Array("Average_Sentiment_Polarity"))

    //df_1.printSchema()
    //df_1.show(10)

    df_1
  }

  def part2(dataframe: DataFrame, path:String): Unit = {
    // Sort in descending order the apps with rating >= 4.0 (and <= 5.0, since is the max a app can have and csv has errors, like a random 19)
    val df_2 = dataframe
      .withColumn("Rating", col("Rating").cast("Double"))
      .filter(col("Rating") >= 4.0 && col("Rating") <= 5.0)
      .sort(col("Rating").desc)

    df_2.write
      .mode(SaveMode.Overwrite)
      .options(Map("header" -> "true", "encoding"->"UTF-8", "delimiter"->"§"))
      .csv(path)

    //df_2.printSchema()
    //df_2.show(10)
  }

  def part3(dataframe: DataFrame): DataFrame = {
    // Replace the "Size" with the MB value converted
    val replace_size = udf((value: String) => {
      if (value.endsWith("M")) {
        value.substring(0, value.length - 1)
      } else if (value.endsWith("k")) {
        value.substring(0, value.length - 1).toDouble./(1024.0).toString // 1 kbytes => 1/1024 Mbytes
      } else {
        null // Varies with device
      }
    })

    // Convert the "Price" to euros
    val dollars_to_euros = udf((value: String) => {
      if (value.startsWith("$")) {
        value.substring(1, value.length).toDouble * 0.9
      }
       else {
        0
      }
    })

    // Create a "Genre" list
    val genres_to_list = udf((value: String) => {
      value.split(";").map(_.trim).toList
    })

    // Convert String to Date
    val to_date = udf((value: String) => {
      value.patch(6, "20", 0)
      val replaced = value.replaceAll("/", "-")
      replaced
    })

    // Create an array of categories
    val categoryArray = array_distinct(collect_list("Category")).as("Categories")

    // Window to order partitioned apps by reviews
    val window_aux = Window.partitionBy("App").orderBy(desc("Reviews"))

    val df_3_1 = dataframe
      .withColumn("row_number", row_number().over(window_aux))
      .filter(col("row_number") === 1)
      .drop("row_number")
      .drop("Category")
      .withColumn("Size", replace_size(col("Size")).cast("Double"))
      .withColumn("Price", dollars_to_euros(col("Price")))
      .withColumn("Genres", genres_to_list(col("Genres")))
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumn("Last_Updated", to_date(col("Last_Updated")))
      .na.fill(0, Array("Reviews"))

    val df_3_2 = dataframe
      .groupBy("App")
      .agg(categoryArray)

    val df_3 = df_3_2.join(df_3_1, Seq("App"), "inner")

    //df_3.printSchema()
    //df_3.show(200)

    df_3
  }

  def part4(df_1: DataFrame, df_3: DataFrame, path: String): DataFrame = {
    val df_3_plus_1 = df_3.join(df_1, Seq("App"), "left") // right is null when match not found

    //df_3_plus_1.printSchema()
    //df_3_plus_1.show(150)

    df_3_plus_1.write
      .mode(SaveMode.Overwrite)
      .options(Map("header"->"true", "compression"->"gzip"))
      .parquet(path)

    df_3_plus_1
  }

  def part5(df_3: DataFrame, df_user_reviews: DataFrame, path: String): Unit = {
    // Select "App" and "Sentiment_Polarity"
    val df_user_reviews_aux = df_user_reviews.select("App","Sentiment_Polarity")

    // Explode "Genres" in multiple rows "Genre"
    val df_3_aux = df_3.select(col("App"), explode(col("Genres")).as("Genre"),col("Rating"))

    val df_4 = df_3_aux
      .join(df_user_reviews_aux, Seq("App"), "left") // // right is null when match not found
      .groupBy("Genre")
      .agg(count("Genre").alias("Count"), avg("Rating").alias("Average_Rating"), avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    //df_4.printSchema()
    //df_4.show(150)

    df_4.write
      .mode(SaveMode.Overwrite)
      .options(Map("header" -> "true", "compression" -> "gzip"))
      .parquet(path)
  }
  def main(args : Array[String]): Unit = {
    val google_apps_folder = "google-play-store-apps"
    val apps_file = google_apps_folder + "/googleplaystore.csv"
    val user_reviews_file = google_apps_folder + "/googleplaystore_user_reviews.csv"

    // Create spark session
    val spark: SparkSession = SparkSession.builder()
      .appName("CSVtoDataFrame")
      .master("local[4]") // 4 cpu cores
      .getOrCreate()

    // Convert CSV file (user_reviews) to DataFrame
    val dataframe_user_reviews = spark.read
      .options(Map("header" -> "true", "delimiter" -> ",", "quote" -> "\"", "escape" -> "\"", "nullValue" -> "nan"))
      .csv(user_reviews_file)

    dataframe_user_reviews.cache() // -> part 5

    // Convert CSV file (apps) to DataFrame
    val dataframe_apps = spark.read
      .options(Map("header" -> "true", "delimiter" -> ",", "quote" -> "\"", "escape" -> "\"", "nullValue" -> "NaN"))
      .csv(apps_file)

    // PART 1
    val df_1 = part1(dataframe_user_reviews)
    df_1.cache() // -> part 4

    // PART 2
    part2(dataframe_apps, google_apps_folder + "/best_apps.csv")

    // PART 3
    val df_3 = part3(dataframe_apps)
    df_3.cache() // -> part 4

    // PART 4
    val df_3_plus_1 = part4(df_1, df_3, google_apps_folder + "/googleplaystore_cleaned")

    // PART 5
    part5(df_3, dataframe_user_reviews, google_apps_folder + "/googleplaystore_metrics")

    // Close spark sessions (good practice)
    spark.stop()
  }

}


