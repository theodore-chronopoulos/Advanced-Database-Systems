from select import select
from pyspark.sql import SparkSession
from datetime import datetime


spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")

def desc_length(average):
    average_len = len(average)
    return average_len

def format_year(datetimevar):
    if (datetimevar == None): 
        year = ''
    else:
        year = str(datetimevar.strftime("%Y"))
    return year

def time_period_year_func(yearvar):
    temp = int(yearvar)
    if (temp < 2005 and temp > 1999):
        ans = "2000-2004"
    elif (temp < 2010 and temp > 2004):
        ans = "2005-2009"
    elif (temp < 2015 and temp > 2009):
        ans = "2010-2014"
    elif (temp < 2020 and temp > 2014):
        ans = "2015-2019"
    else:
        ans =  ''
    return ans

movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
movie_genres.printSchema()
movie_genres.registerTempTable("movie_genres")

movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
movies.printSchema()
movies.registerTempTable("movies")

spark.udf.register("format_year", format_year)
spark.udf.register("desc_length", desc_length)
spark.udf.register("time_period_year_func", time_period_year_func)

sqlString1 = \
    "select movie_genres.ID as Drama_Movies_ID "  + \
	"from movie_genres " + \
	"where movie_genres.Genre ==  'Drama' "

sqlString2 = \
    "select desc_length(movies.Description) as Desc_Length, format_year(movies.Release_Date) as Year "  + \
	"from movies, drama_movies " + \
    "where drama_movies.Drama_Movies_ID == movies.ID and movies.Description <> '' and format_year(movies.Release_Date) > '1999'"

sqlString3 = \
    "select Desc_Length as Desc_Length, time_period_year_func(Year) as Time_Period "  + \
	"from desc_length "
    
sqlString4 = \
    "select avg(desc_length_per_5.Desc_Length) as Avg_Length, desc_length_per_5.Time_Period as Time_Period "  + \
	"from desc_length_per_5 " + \
    "group by desc_length_per_5.Time_Period" 

res1 = spark.sql(sqlString1)
res1.registerTempTable("drama_movies")
res1.show()

res2 = spark.sql(sqlString2)
res2.registerTempTable("desc_length")
res2.show()

res3 = spark.sql(sqlString3)
res3.registerTempTable("desc_length_per_5")
res3.show()

res4 = spark.sql(sqlString4)
res4.show()