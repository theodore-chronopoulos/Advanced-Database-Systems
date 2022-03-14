from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField,, IntegerType, DoubleType


spark = SparkSession.builder.appName(
    "query3-rd").getOrCreate()

table_schema = StructType([StructField('User', IntegerType()),
                     StructField('Movie_ID', IntegerType()),
                     StructField('Rating',DoubleType()),
                     StructField('Moment', IntegerType())])

df = spark.read.csv('hdfs://master:9000/files/ratings.csv', sep=',',
                         schema = table_schema, enforceSchema = True)
# This command was used for using the correct types of the schema variables
# df = spark.read.csv('hdfs://master:9000/files/ratings.csv', sep=',',
#                          inferSchema = True)

df.show()

df.write.parquet("hdfs://master:9000/files/ratings.parquet")
spark.read.parquet("hdfs://master:9000/files/ratings.parquet").show()

df.printSchema()