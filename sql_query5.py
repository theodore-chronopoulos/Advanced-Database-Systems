from select import select
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")

movie_genres = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/files/movie_genres.csv")

movie_genres.registerTempTable("movie_genres")

movies = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/files/movies.csv")

movies.registerTempTable("movies")

ratings = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/files/ratings.csv")

ratings.registerTempTable("ratings")

sqlString1 = \
    "select movie_genres._c0 as Movie_ID, movie_genres._c1 as Genre, ratings._c0 as User_ID, ratings._c2 as Rating "  + \
	"from movie_genres, ratings " + \
	"where movie_genres._c0 ==  ratings._c1 "

sqlString2 = \
    "select count(User_ID) as Total_User_Rating, Genre, User_ID "  + \
	"from rating_with_genre " + \
    "group by Genre, User_ID " + \
    "order by Total_User_Rating desc"

sqlString3 = \
    "select max(Total_User_Rating) as Max_User_Ratings_Per_Cat, Genre "  + \
	"from user_ratings_per_cat " + \
    "group by Genre"
    
sqlString4 = \
    "select Total_User_Rating, user_ratings_per_cat.Genre as Genre, User_ID "  + \
	"from max_number_of_ratings_per_cat, user_ratings_per_cat " + \
    "where max_number_of_ratings_per_cat.Genre == user_ratings_per_cat.Genre " + \
    "and max_number_of_ratings_per_cat.Max_User_Ratings_Per_Cat == user_ratings_per_cat.Total_User_Rating"

sqlString5 = \
	"select Total_User_Rating, user_with_max_rating_per_cat.Genre as Genre, user_with_max_rating_per_cat.User_ID as User_ID, " + \
	"Rating, Movie_ID " + \
	"from user_with_max_rating_per_cat, rating_with_genre " + \
	"where rating_with_genre.User_ID == user_with_max_rating_per_cat.User_ID " + \
	"and rating_with_genre.Genre == user_with_max_rating_per_cat.Genre"

sqlString6 = \
	"select Total_User_Rating, Genre, User_ID, " + \
	"Rating, Movie_ID, movies._c7 as Popularity, movies._c1 as Title " + \
	"from final0, movies " + \
	"where Movie_ID == movies._c0 "

sqlString7 = \
	"select Genre, max(Rating) as Max " + \
	"from final1 " + \
	"group by Genre "

sqlString8 = \
	"select Genre, min(Rating) as Min " + \
	"from final1 " + \
	"group by Genre "

sqlString9 = \
	"select final2.Genre as Genre, Min, Max " + \
	"from final2, final3 " + \
	"where final2.Genre == final3.Genre "

sqlString10 = \
	"select final1.Genre as Genre, Min, Max, " + \
	"Total_User_Rating, User_ID, " + \
	"Rating, Movie_ID, Popularity, Title " + \
	"from min_max, final1 " + \
	"where min_max.Genre == final1.Genre and (Min == Rating or Max == Rating) "

res1 = spark.sql(sqlString1)
res1.registerTempTable("rating_with_genre")
res1.show()

res2 = spark.sql(sqlString2)
res2.registerTempTable("user_ratings_per_cat")
res2.show()

res3 = spark.sql(sqlString3)
res3.registerTempTable("max_number_of_ratings_per_cat")
res3.show()

res4 = spark.sql(sqlString4)
res4.registerTempTable("user_with_max_rating_per_cat")
res4.show()

res5 = spark.sql(sqlString5)
res5.registerTempTable("final0")
res5.show()

res6 = spark.sql(sqlString6)
res6.registerTempTable("final1")
res6.show()

res7 = spark.sql(sqlString7)
res7.registerTempTable("final2")
res7.show()

res8 = spark.sql(sqlString8)
res8.registerTempTable("final3")
res8.show()

res9 = spark.sql(sqlString9)
res9.registerTempTable("min_max")
res9.show()

res10 = spark.sql(sqlString10)
res10.registerTempTable("final")
res10.show()