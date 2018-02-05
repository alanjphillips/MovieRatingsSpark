package com.alaphi.movieratings

import java.io.File.separator
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{asc, avg, max, min}

object MovieRatings {

  case class Movie(id: Int, title: String, genres: String)

  case class Rating(userId: Int, movieId: Int, stars: Int, timestamp: Long)

  def asMovie(line: String): Movie = {
    val movieAttrs = line.split("::")
    Movie(movieAttrs(0).toInt, movieAttrs(1).trim, movieAttrs(2).trim)
  }

  def asRating(line: String): Rating = {
    val ratingAttrs = line.split("::")
    Rating(ratingAttrs(0).toInt, ratingAttrs(1).toInt, ratingAttrs(2).toInt, ratingAttrs(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Folders, Files, Params.. All of these could be supplied as cmd line params.
    val dataFolder = sys.env("HOME") + separator + "moviedata" + separator
    val ratingResultsPath = dataFolder + separator + "parquet" + separator + "movieRatings_"
    val top3ResultsPath = dataFolder + separator + "parquet" + separator + "userTop3_"
    val moviesFile = "movies.dat"
    val ratingsFile = "ratings.dat"

    // Spark Conf and Context
    val conf = new SparkConf().setAppName("MovieRatings")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Load and calculate agg Ratings
    val ratingsRaw = sc.textFile(dataFolder + ratingsFile)
    val ratings = ratingsRaw.map(asRating).toDF().cache()

    val aggRating =
      ratings
        .groupBy("movieId")
        .agg(
          max("stars") as "maxStars",
          min("stars") as "minStars",
          avg("stars") as "avgStars"
        )

    // Load movies data
    val moviesRaw = sc.textFile(dataFolder + moviesFile)
    val movies = moviesRaw.map(asMovie).toDF().cache()

    // Join on movieId
    val moviesRatings =
      movies
        .join(aggRating, movies("id") === aggRating("movieId"))
        .drop(aggRating("movieId"))
        .orderBy(desc("avgStars"), asc("id"))

    // Output file and print to console
    val nowSuffix = LocalDateTime.now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    moviesRatings.write.parquet(ratingResultsPath + nowSuffix)
    moviesRatings.show(100)

    // Top 3 movies for each user
    val userTop3 =
      ratings
        .join(movies, movies("id") === ratings("movieId"))
        .select("userId", "stars", "title")
        .rdd.map(
          r => r.getInt(0) -> (r.getInt(1), r.getString(2))             // userId -> (stars ,movieId)
        )
        .groupByKey()                                                   // userId -> Seq((stars ,movieId),...)
        .mapValues(starMovieSeq =>
          starMovieSeq.toList.sortWith(_._1 > _._1).map(_._2).take(3)   // sort (stars ,movieId) tuples by stars count and take top 3
        )
        .toDF

    println("Top 3 movies for each user")
    userTop3.write.parquet(top3ResultsPath + nowSuffix)
    userTop3.show(100, false)

    sqlContext.sparkContext.stop()
  }

}
