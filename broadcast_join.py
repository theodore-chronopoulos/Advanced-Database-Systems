from pyspark.sql import SparkSession
from io import StringIO
import csv

def split_complex(x):
 	return list(csv.reader(StringIO(x), delimiter=','))[0]

# Function that creates a hash table given a small dataset as list of lists
# with join_key as hash_table key. This table will be stored in memory  
def constructHashTable(lines,join_key):
    hash_table = {}
    for line in lines:
        splitted = split_complex(line)
        key = splitted[join_key]
        val = splitted[:join_key] + splitted[join_key+1:]
        if key not in hash_table: hash_table[key] = [val]
        else: hash_table[key].append(val)
    return hash_table

# Probe hash_table for scanned rows join key . If stored rows exists in the 
# hash table then spit the join of each of them with the given row.
# This function is used inside a flatMap Transformation so it returns more than
# one row for every scanned row
def probeHashTable(row):
    global hash_table, large_join_key
    hash_table_key = row[large_join_key]
    if hash_table_key in hash_table:
        row_without_shared_key = row[:large_join_key] + row[large_join_key+1:]
        all_stored_rows = hash_table[hash_table_key]
        joined_rows = [] 
        for stored_row in all_stored_rows: 
            joined_rows.append(( hash_table_key, row_without_shared_key + stored_row))
        return joined_rows
    return []

spark = SparkSession.builder.appName("broadcast_join").getOrCreate()
sc = spark.sparkContext
sc.addFile('./functions.py')

# Broadcast small dataset
small_dataset = sc.textFile("/files/movie_genres.csv")
small_dataset_broadcasted = sc.broadcast(small_dataset.collect())

small_join_key,large_join_key = 0 , 1
hash_table = constructHashTable(small_dataset_broadcasted.value, small_join_key)

# Scan large dataset and probe small hash table for rows with the same join key
large_dataset = sc.textFile("/files/ratings.csv")
large_dataset_rows = large_dataset.map(lambda x: split_complex(x))
joined = large_dataset_rows.flatMap(lambda x: probeHashTable(x))
final = joined.map(lambda x: (x[0], *x[1]))

# Write RDD to HDFS as CSV
writeRddToCSV(spark,final,'ratings_genres_broadcast')