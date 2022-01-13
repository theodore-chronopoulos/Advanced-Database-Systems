from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,DoubleType,LongType


spark = SparkSession.builder.appName(
    "query1-rd").getOrCreate()


table_schema = StructType([StructField('ID', IntegerType()),
                     StructField('Title', StringType()),
                     StructField('Description', StringType()),
                     StructField('Release_Date', TimestampType()),
                     StructField('Duration', DoubleType()),
                     StructField('Cost', IntegerType()),
                     StructField('Income', LongType()),
                     StructField('Popularity', DoubleType())])

df = spark.read.csv('hdfs://master:9000/files/movies.csv', sep=',',
                         schema = table_schema, enforceSchema = True)
# This command was used for using the correct types of the schema variables
# df = spark.read.csv('hdfs://master:9000/files/movies.csv', sep=',',
#                          inferSchema = True)

print("here")
df.show()

df.write.parquet("hdfs://master:9000/files/movies.parquet")
spark.read.parquet("hdfs://master:9000/files/movies.parquet").show()

df.printSchema()