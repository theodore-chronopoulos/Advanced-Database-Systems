#show hadoop
hadoop fs -ls hdfs://master:9000/<folder_path>
#delete hadoop
hadoop fs -rm -r /user/test/sample.txt
#spark submit
spark-submit myscript.py <script_arg1> <script_arg2> … <script_argN>
#copy from hdfs to local
hdfs dfs -copyToLocal hdfs://master:9000/outputs /home/user/outputs
#copy from hdfs to local and merge
hadoop fs -getmerge hdfs://master:9000/outputs /home/user/outputs
#copy from remote to local
scp -r user@83.212.78.155:~/outputs/ ~/Desktop/Advanced-Database-Systems/outputs
#upload to hadoop
hadoop fs -put <file_name> hdfs://master:9000/<rest_path_to_upload>