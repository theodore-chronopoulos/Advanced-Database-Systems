from select import select
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col
import timeit

start = timeit.default_timer()

spark = SparkSession.builder.appName("query1-rdd").getOrCreate()
spark.conf.set("spark.sql.crossJoin.enabled", "true")

def format_year(datetimevar):
    if (datetimevar == None): 
        year = ''
    else:
        year = str(datetimevar.strftime("%Y"))
    return year

def profit_func(cost, income):
    temp1 = (income - cost) * 100
    profit = temp1 / cost
    return profit

movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
movies.printSchema()

movies.registerTempTable("movies")
spark.udf.register("formatter", format_year)
spark.udf.register("profit", profit_func)


sqlString1 = \
    "select Title, formatter(Release_Date) as Year, profit(Cost, Income) as Profit  "  + \
	"from movies " + \
    "where Cost <> 0 and Income <> 0 and formatter(Release_Date) > '1999' " + \
    "order by Year desc "

sqlString2 = \
    "select Year, max(Profit) as Profit  "  + \
	"from profits " + \
    "group by Year "
    
sqlString3 = \
    "select maxes.Year as Year, profits.Title as Movie, profits.Profit as Profit "  + \
	"from maxes, profits " + \
    "where  maxes.Year == profits.Year and maxes.Profit == profits.Profit " + \
    "order by maxes.Year desc "

res = spark.sql(sqlString1)
res = res.withColumn("Profit",res.Profit.cast('double'))
res.registerTempTable("profits")

res2 = spark.sql(sqlString2)
res2.registerTempTable("maxes")
res3 = spark.sql(sqlString3)
# res3.show()

res3.write.csv("hdfs://master:9000/outputs/sql_parquet_q1.csv")

stop = timeit.default_timer()
print('Time: ', stop - start) 