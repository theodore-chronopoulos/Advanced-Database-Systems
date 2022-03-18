from ast import Lambda
from ntpath import join
from tokenize import Double
from pyspark.sql import SparkSession
from io import StringIO
import csv
import timeit

start = timeit.default_timer()

def split_complex(x):
 	return list(csv.reader(StringIO(x), delimiter=','))[0]

def format_year(year):
	fName = year.split("-")[0]
	return fName

spark = SparkSession.builder.appName("query5-rdd").getOrCreate()

sc = spark.sparkContext


rdd1 = \
	sc.textFile("hdfs://master:9000/files/movies.csv"). \
	map(lambda x : split_complex(x))
rdd2 = \
	sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
	map(lambda x : split_complex(x))
rdd3 = \
	sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	map(lambda x : split_complex(x))

rddmapmovies = rdd1.map ( lambda x:(int(x[0]),(x[1],float(x[7])))) #movie_id, movie_title,popularity
rdd22 = rdd2.map ( lambda x : (int(x[0]),x[1])) # movie id, genre
rdd33 = rdd3.map ( lambda x: (int(x[1]),(int(x[0]),float(x[2])))) #movie id, user,rating

rdd4 = rdd33.join(rdd22)
rdd5 = rdd4.map( lambda x: ((x[1][1],x[1][0][0]),1)) # ((genre,user),(1, movie id, rating)
rdd55 = rdd4.map( lambda x: ((x[1][1],x[1][0][0]), (x[0],x[1][0][1]))) # ((genre,user),(movie id, rating)
rdd6 = rdd5.reduceByKey(lambda x,y:(x+y)) #((genre, user), total_ratings)
rdd7 = rdd6.map(lambda x:(x[0][0],(x[0][1],x[1]))) #(genre, list(user, ratings))
rdd8 = rdd7.reduceByKey(lambda x,y: ( x if(x[1]>y[1]) else y)) #(genre, (user, max_total_ratings))
rdd9 = rdd8.map(lambda x: ((x[0],x[1][0]), (x[1][1]))) # ((genre, user), max_total_ratings)
rdd10 = rdd9.join(rdd55) #((genre,user_with_max_number),list_of(movie id, rating)

rdd10map = rdd10.map(lambda x:(x[1][1][0],(x[0][0],x[0][1],x[1][0],x[1][1][1])))
						#(movie id, genre,user,numberofratings,rating)

#joining rdd10map and rddmoviesmap so we have all the information we need

rdd_joined_info = rdd10map.join(rddmapmovies)
rdd_map_joined_info = rdd_joined_info.map( lambda x:((x[1][0][0],x[1][0][1],x[1][0][3]),(x[1][0][2],x[1][1][0],x[1][1][1])))
	#((Genre,user,rating),(numberofratings,movietitle,popularity))

rdd_reduced_joined_info = rdd_map_joined_info.reduceByKey(lambda x,y:(x if(x[2]>y[2])else y))
rdd_max_map = rdd_reduced_joined_info.map(lambda x: ((x[0][0],x[0][1]),(x[0][2],x[1][0],x[1][1],x[1][2])))
rdd_max_final = rdd_max_map.reduceByKey(lambda x,y: x if(x[0]>y[0])else y)

rdd_min_final = rdd_max_map.reduceByKey(lambda x,y: x if(x[0]<y[0])else y)

rdd_all_results = rdd_max_final.join(rdd_min_final)
rdd_all_results_map = rdd_all_results.map(lambda x: (x[0][0],x[0][1],x[1][0][1],x[1][0][2],x[1][0][0],x[1][1][2],x[1][1][0]))

rdd_result = rdd_all_results_map.sortBy(lambda x: x[0])

# for i in rdd5.take(10):
#     print(i)
# for i in rdd55.take(10):
#     print(i)
def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = rdd_result.map(toCSVLine)
lines.saveAsTextFile('hdfs://master:9000/outputs/rdd_q5.csv')

stop = timeit.default_timer()
print('Time: ', stop - start) 

