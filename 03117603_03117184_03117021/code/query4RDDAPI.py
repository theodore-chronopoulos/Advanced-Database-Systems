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

def group_year(year):
	str1 = '2000-2004'
	str2 = '2005-2009'
	str3 = '2010-2014'
	str4 = '2015-2019'
	if (year < 2005 and year > 1999):
		return str1
	if (year < 2010 and year > 2004):
		return str2
	if (year < 2015 and year > 2009):
		return str3
	if (year < 2020 and year > 2014):
		return str4

spark = SparkSession.builder.appName("query4-rdd").getOrCreate()

sc = spark.sparkContext


rdd = \
	sc.textFile("hdfs://master:9000/files/movies.csv"). \
	map(lambda x : split_complex(x))
rdd1 = \
	sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
	map(lambda x : split_complex(x))

rdd2 = rdd.map( lambda x : (x[0],x[2],format_year(x[3]))) #movie id, perilipsi, timestamp
rdd22 = rdd2.filter( lambda x: x[1]!= '' and x[2] != '' and int(x[2]) >1999) #na min einai keni i perilipsi
rdd222 = rdd22.map( lambda x: (x[0],(group_year(int(x[2])),x[1])))

rdd3 = rdd1.map( lambda x: (x[0],x[1])) #movie id, genre)
rdd4 = rdd3.filter( lambda x: (x[1]== 'Drama'))
rdd5 = rdd222.join(rdd4)
rdd6 = rdd5.map( lambda x: (x[1][0][0],(1,len(x[1][0][1].split()))))
rdd7 = rdd6.reduceByKey( lambda x,y: (x[0]+y[0],x[1]+y[1]))
rdd8 = rdd7.map(lambda x: (x[0],x[1][1]/x[1][0]))

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = rdd8.map(toCSVLine)
lines.saveAsTextFile('hdfs://master:9000/outputs/rdd_q4.csv')

# for i in rdd9.take(1):
# 	print(i)

stop = timeit.default_timer()
print('Time: ', stop - start) 