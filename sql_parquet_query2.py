from select import select
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")

def average_func(average):
    average = round(average, 2)
    return average

def percentage_func(plusthree, total):
    average = (plusthree / total) * 100
    average = round(average, 2)
    return average

ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
ratings.printSchema()

ratings.registerTempTable("ratings")
spark.udf.register("average_func", average_func)
spark.udf.register("percentage_func", percentage_func)

sqlString1 = \
    "select _c0 as User_ID, average_func(avg(_c2)) as Avg_Stars "  + \
	"from ratings " + \
    "group by User_ID "

sqlString2 = \
    "select count(User_ID) as Num_of_Users_3plus "  + \
	"from average_ratings " + \
    "where  Avg_Stars > 3 "

sqlString3 = \
    "select count(User_ID) as Num_of_Users "  + \
	"from average_ratings " 

sqlString4 = \
    "select percentage_func(num_of_users_3plus.Num_of_Users_3plus, num_of_users.Num_of_Users) as Percentage "  + \
	"from num_of_users_3plus, num_of_users " 

res = spark.sql(sqlString1)
res.registerTempTable("average_ratings")
res.show()
res2 = spark.sql(sqlString2)
res2.registerTempTable("num_of_users_3plus")
res2.show()
res3 = spark.sql(sqlString3)
res3.registerTempTable("num_of_users")
res3.show()
res4 = spark.sql(sqlString4)
res4.show()
