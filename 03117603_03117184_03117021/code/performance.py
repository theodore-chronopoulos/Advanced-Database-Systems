import numpy as np
import matplotlib.pyplot as plt
 
# set width of bar
barWidth = 0.25
fig = plt.subplots(figsize =(12, 8))

# [query1, query2, query3, query4, query5] in seconds
rdd_array = [8.449, 141.898, 140.198, 10.697, 135.278]
spark_csv_array = [24.171, 108.698, 115.496, 23.65, 218.05]
spark_parquet_array = [20.17, 22.603, 30.182, 19.805, 129.55]
 
# Set position of bar on X axis
br1 = np.arange(len(rdd_array))
br2 = [x + barWidth for x in br1]
br3 = [x + barWidth for x in br2]
 
# Make the plot
plt.bar(br1, rdd_array, color ='r', width = barWidth,
        edgecolor ='grey', label ='rdd')
plt.bar(br2, spark_csv_array, color ='g', width = barWidth,
        edgecolor ='grey', label ='sql_csv')
plt.bar(br3, spark_parquet_array, color ='b', width = barWidth,
        edgecolor ='grey', label ='sql_parquet')
 
# Adding Xticks
plt.xlabel('Implementations of queries', fontweight ='bold', fontsize = 15)
plt.ylabel('Time in seconds', fontweight ='bold', fontsize = 15)
plt.xticks([r + barWidth for r in range(len(rdd_array))],
        ['query1', 'query2', 'query3', 'query4', 'query5'])
 
plt.legend()
plt.show()
