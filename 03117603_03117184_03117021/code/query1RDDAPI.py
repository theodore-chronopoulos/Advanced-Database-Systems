
from pyspark.sql import SparkSession
from io import StringIO
import csv
import timeit

start = timeit.default_timer()

def split_complex(x):
 	return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

def format_year(year):
	fName = year.split("-")[0]
	return fName

rdd = \
	sc.textFile("hdfs://master:9000/files/movies.csv"). \
	map(lambda x : split_complex(x))

rdd2 = rdd.map(lambda x : (x[3], format_year(x[3]),int(x[5]),int(x[6]),x[1]))

rdd3 = rdd2.filter(lambda x : x[1] != '' and int(x[1]) > 1999 and int(x[2])>0 and int(x[3])>0) 

rdd4 = rdd3.map(lambda x : (x[1],(x[4], ((x[3] - x[2])*100)/x[2])))


rdd5 = rdd4.groupBy(lambda x: x[1]).mapValues(list)

rdd6 = rdd5.reduceByKey(lambda x: x[1])

result = rdd4.reduceByKey(lambda acc,value: (acc if(acc[1] > value[1]) else value))

# for i in result.collect():
# 	print(i)

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = result.map(toCSVLine)
lines.saveAsTextFile('hdfs://master:9000/outputs/rdd_q1.csv')


stop = timeit.default_timer()
print('Time: ', stop - start) 
