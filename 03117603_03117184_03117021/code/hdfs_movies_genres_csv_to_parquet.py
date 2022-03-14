from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField, StringType, IntegerType


spark = SparkSession.builder.appName(
    "query2-rd").getOrCreate()


table_schema = StructType([StructField('ID', IntegerType()),
                     StructField('Genre', StringType())])
                     

df = spark.read.csv('hdfs://master:9000/files/movie_genres.csv', sep=',',
                         schema = table_schema, enforceSchema = True)
# This command was used for using the correct types of the schema variables
# df = spark.read.csv('hdfs://master:9000/files/movie_genres.csv', sep=',',
#                          inferSchema = True)

print("here")
df.show()

df.write.parquet("hdfs://master:9000/files/movie_genres.parquet")
spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet").show()

df.printSchema()