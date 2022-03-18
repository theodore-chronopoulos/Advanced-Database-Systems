from ntpath import join
from tokenize import Double
from pyspark.sql import SparkSession

from io import StringIO
import csv
import timeit

start = timeit.default_timer()

def split_complex(x):
 	return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("query3-rdd").getOrCreate()

sc = spark.sparkContext


rdd = \
	sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	map(lambda x : split_complex(x))
rdd1 = \
	sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
	map(lambda x : split_complex(x))

rdd2 = rdd.map( lambda x : (int(x[1]), (1,float(x[2])))) #movie id,rating
rdd3 = rdd2.reduceByKey( lambda x,y : (x[0] + y[0],x[1] +y[1]))
rdd4 = rdd3.map( lambda x: (int(x[0]),(1,x[1][1]/x[1][0])))
rdd5 = rdd1.map( lambda x: (int(x[0]),x[1]))
rdd6 = rdd5.join(rdd4)
rdd7 = rdd6.map ( lambda x: (x[1][0],x[1][1]))
rdd8 = rdd7.reduceByKey( lambda x,y: (x[0]+y[0],x[1]+y[1]))
rdd9 = rdd8.map(lambda x: (x[0],x[1][1]/x[1][0],x[1][0]))


# for i in rdd9.take(15):
#     print(i)

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = rdd9.map(toCSVLine)
lines.saveAsTextFile('hdfs://master:9000/outputs/rdd_q3.csv')


stop = timeit.default_timer()
print('Time: ', stop - start) 


