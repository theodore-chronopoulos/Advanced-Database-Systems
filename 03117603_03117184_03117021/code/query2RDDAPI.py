from ntpath import join
from tokenize import Double
from pyspark.sql import SparkSession

import timeit

start = timeit.default_timer()

from io import StringIO
import csv
def split_complex(x):
 	return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName("query2-rdd").getOrCreate()

sc = spark.sparkContext

rdd = \
	sc.textFile("hdfs://master:9000/files/ratings.csv"). \
	map(lambda x : split_complex(x))

rdd1 = rdd.map(lambda x : (int(x[0]),int(x[1]),float(x[2])))
rdd2 = rdd1.map(lambda x : (x[0],(1,x[2])))

rdd3 = rdd2.reduceByKey(lambda x,y: (x[0]+y[0],x[1] + y[1]))
rdd4 = rdd3.map(lambda x : (1,(1,x[1][1]/x[1][0])))
rdd5 = rdd4.filter(lambda x: x[1][1]>3)
rdd6 = rdd5.reduceByKey(lambda x,y: (x[0]+y[0],"a"))
rdd7 = rdd4.reduceByKey(lambda x,y: (x[0]+y[0],"b"))
rdd8 = rdd6.join(rdd7)
rdd9 = rdd8.map(lambda x: 100 * x[1][0][0] / x[1][1][0])

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = rdd9.map(toCSVLine)
lines.saveAsTextFile('hdfs://master:9000/outputs/rdd_q2.csv')

# for i in rdd2.take(2):
# 	print(i)

for i in rdd9.take(1):
	print(i)


stop = timeit.default_timer()
print('Time: ', stop - start) 