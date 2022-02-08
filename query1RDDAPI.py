from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()

sc = spark.sparkContext

def format_name(name):
	fName = name.split(" ")[0]
	lName = name.split(" ")[1]
	return lName + " " + fName[0] + "."

res = \
	sc.textFile("hdfs://master:9000/files/movies.csv"). \
	map(lambda x : x.split(",")) \
	take(5)

for i in res:
	print(i)