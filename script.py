from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "query1-rd").getOrCreate()
print("aaaaaaaa")
df = spark.read.csv('hdfs://master:9000/files/movies.csv', sep=',',
                         inferSchema=True)

print("here")
df.show()

df.write.parquet("hdfs://master:9000/files/movies.parquet")