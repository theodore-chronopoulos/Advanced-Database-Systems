from select import select
from pyspark.sql import SparkSession
import timeit

start = timeit.default_timer()

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")

def percentage_func(plusthree, total):
    average = (plusthree / total) * 100
    return average

ratings = spark.read.format('csv'). \
			options(header='false',
				inferSchema='true'). \
			load("hdfs://master:9000/files/ratings.csv")

ratings.registerTempTable("ratings")
spark.udf.register("percentage_func", percentage_func)

sqlString1 = \
    "select _c0 as User_ID, avg(_c2) as Avg_Stars "  + \
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
# res.show()
res2 = spark.sql(sqlString2)
res2.registerTempTable("num_of_users_3plus")
# res2.show()
res3 = spark.sql(sqlString3)
res3.registerTempTable("num_of_users")
# res3.show()
res4 = spark.sql(sqlString4)
# res4.show()
res4.write.csv("hdfs://master:9000/outputs/sql_csv_q2.csv")

stop = timeit.default_timer()
print('Time: ', stop - start) 