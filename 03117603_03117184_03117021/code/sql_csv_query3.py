from select import select
from pyspark.sql import SparkSession
import timeit

start = timeit.default_timer()

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")

movie_genres = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/files/movie_genres.csv")

movie_genres.registerTempTable("movie_genres")

ratings = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/files/ratings.csv")

ratings.registerTempTable("ratings")

sqlString1 = \
    "select _c1 as Movie_ID, avg(_c2) as Avg_Stars "  + \
	"from ratings " + \
    "group by Movie_ID "

sqlString2 = \
    "select count(movie_genres._c0) as Movies_Per_Category, movie_genres._c1 as Genre "  + \
	"from movie_genres, avg_per_movie " + \
	"where movie_genres._c0 ==  avg_per_movie.Movie_ID " + \
    "group by Genre "

sqlString3 = \
    "select avg(avg_per_movie.Avg_Stars) as Avg_per_Cat, movie_genres._c1 as Genre "  + \
	"from avg_per_movie, movie_genres " + \
    "where avg_per_movie.Movie_ID == movie_genres._c0 " + \
    "group by movie_genres._c1" 

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
# res1.show()
# res2.show()
# res3.show()
# res4.show()
res4.write.csv("hdfs://master:9000/outputs/sql_csv_q3.csv")

stop = timeit.default_timer()
print('Time: ', stop - start) 