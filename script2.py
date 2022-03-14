from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,DoubleType,LongType


spark = SparkSession.builder.appName(
    "query1-rd").getOrCreate()


table_schema = StructType([StructField('ID', IntegerType(), True),
                     StructField('Title', StringType(), True),
                     StructField('Description', StringType(), False),
                     StructField('Release Date', TimestampType(), False),
                     StructField('Duration', DoubleType(), True),
                     StructField('Cost', IntegerType(), True),
                     StructField('Income', LongType(), True),
                     StructField('Popularity', DoubleType(), True)])

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