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
	"select Genre, max(Rating) as Max, min(Rating) as Min " + \
	"from final1 " + \
	"group by Genre "

sqlString8 = \
	"select final1.Genre as Genre, max(Popularity) as Max_Pop " + \
	"from min_max, final1 " + \
	"where min_max.Genre == final1.Genre and Max == Rating " + \
	"group by final1.Genre "

sqlString82 = \
	"select final1.Genre as Genre, max(Popularity) as Min_Pop " + \
	"from min_max, final1 " + \
	"where min_max.Genre == final1.Genre and Min == Rating " + \
	"group by final1.Genre "

sqlString9 = \
	"select min_max.Genre as BGenre, " + \
	"Rating as BRating, Title as Best_Movie " + \
	"from min_max, final1, final_max_pop " + \
	"where min_max.Genre == final_max_pop.Genre and min_max.Genre == final1.Genre and " + \
	"min_max.Max == Rating and final_max_pop.Max_Pop == Popularity" 

sqlString10 = \
	"select min_max.Genre as WGenre, Total_User_Rating as Number_of_Ratings, User_ID, " + \
	"Rating as WRating, Title as Worst_Movie " + \
	"from min_max, final1, final_min_pop " + \
	"where min_max.Genre == final_min_pop.Genre and min_max.Genre == final1.Genre and " + \
	"min_max.Min == Rating and final_min_pop.Min_Pop == Popularity" 

sqlString11 = \
	"select WGenre as Genre, User_ID, " + \
	"worst_movie_per_cat.Number_of_Ratings as Number_of_Ratings, Best_Movie, " + \
	"BRating as Best_Rating, Worst_Movie, " + \
	"WRating as Worst_Rating " + \
	"from worst_movie_per_cat, best_movie_per_cat " + \
	"where WGenre == BGenre " + \
	"order by Genre"

res1 = spark.sql(sqlString1)
res1.registerTempTable("rating_with_genre")
# res1.show()

res2 = spark.sql(sqlString2)
res2.registerTempTable("user_ratings_per_cat")
# res2.show()

res3 = spark.sql(sqlString3)
res3.registerTempTable("max_number_of_ratings_per_cat")
# res3.show()

res4 = spark.sql(sqlString4)
res4.registerTempTable("user_with_max_rating_per_cat")
# res4.show()

res5 = spark.sql(sqlString5)
res5.registerTempTable("final0")
# res5.show()

res6 = spark.sql(sqlString6)
res6.registerTempTable("final1")
# res6.show()
# res6.printSchema()

res7 = spark.sql(sqlString7)
res7.registerTempTable("min_max")
# res7.show()

res8 = spark.sql(sqlString8)
res8.registerTempTable("final_max_pop")
# res8.show()

res82 = spark.sql(sqlString82)
res82.registerTempTable("final_min_pop")
# res82.show()

res9 = spark.sql(sqlString9)
res9.registerTempTable("best_movie_per_cat")
# res9.show()

res10 = spark.sql(sqlString10)
res10.registerTempTable("worst_movie_per_cat")
# res10.show()

res11 = spark.sql(sqlString11)
# res11.registerTempTable("final_table")
# res11.show()

res11.write.csv("hdfs://master:9000/outputs/sql_csv_q5.csv")

stop = timeit.default_timer()
print('Time: ', stop - start) 