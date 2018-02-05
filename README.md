# MovieRatingsSpark

Using the movie lens open data set (ml-1m.zip) from http://files.grouplens.org/datasets/movielens/ml-1m.zip

Calculate:
1) The max, min and avg rating for each movie
2) Each user's top 3 movies based on the user's rating

### Build:

> sbt assembly

### Submit to Spark

spark-submit --master local[*] MovieRatingsSpark.jar