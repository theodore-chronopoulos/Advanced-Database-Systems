from select import select
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")

movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
movie_genres.printSchema()
movie_genres.registerTempTable("movie_genres")

ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
ratings.printSchema()
ratings.registerTempTable("ratings")

sqlString1 = \
    "select Movie_ID, avg(Rating) as Avg_Stars "  + \
	"from ratings " + \
    "group by Movie_ID "

sqlString2 = \
    "select count(movie_genres.ID) as Movies_Per_Category, movie_genres.Genre as Genre "  + \
	"from movie_genres, avg_per_movie " + \
	"where movie_genres.ID ==  avg_per_movie.Movie_ID " + \
    "group by Genre "

sqlString3 = \
    "select avg(avg_per_movie.Avg_Stars) as Avg_per_Cat, movie_genres.Genre as Genre "  + \
	"from avg_per_movie, movie_genres " + \
    "where avg_per_movie.Movie_ID == movie_genres.ID " + \
    "group by movie_genres.Genre" 

sqlString4 = \
    "select avg_per_cat.Genre as Genre, avg_per_cat.Avg_per_Cat as Avg_per_Cat, movies_per_cat.Movies_Per_Category as Movies_Per_Category "  + \
	"from avg_per_cat, movies_per_cat " + \
    "where avg_per_cat.Genre == movies_per_cat.Genre " 

res1 = spark.sql(sqlString1)
res1.registerTempTable("avg_per_movie")
res2 = spark.sql(sqlString2)
res2.registerTempTable("movies_per_cat")
res3 = spark.sql(sqlString3)
res3.registerTempTable("avg_per_cat")
res4 = spark.sql(sqlString4)
res1.show()
res2.show()
res3.show()
res4.show()