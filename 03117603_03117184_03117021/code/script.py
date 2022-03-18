from pyspark.sql import SparkSession
import sys, time
disabled = sys.argv[1]
spark = SparkSession.builder.appName('query1-sql').getOrCreate()
if disabled == "Y":
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
elif disabled == 'N':
    pass
else:
    raise Exception ("This setting is not available.")

df1 = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")
df2 = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
df1.registerTempTable("ratings")
df2.registerTempTable("movie_genres")
sqlString = \
"SELECT * " + \
"FROM " + \
" (SELECT * FROM movie_genres LIMIT 100) as g, " + \
" ratings as r " + \
"WHERE " + \
" r.Movie_ID = g.ID"
t1 = time.time()
res = spark.sql(sqlString).collect()
# res = spark.sql(sqlString)
# res.show()
t2 = time.time()
# for i in res:
#     print(i)
print("Time with choosing join type %s is %.4f sec."%("enabled" if
disabled == 'N' else "disabled", t2-t1))
# Εκτέλεση script ως
# -> spark-submit <name>.py Υ για απενεργοποίηση του βελτιστοποιητή
# -> spark-submit <name>.py N για ενεργοποίηση του βελτιστοποιητή
