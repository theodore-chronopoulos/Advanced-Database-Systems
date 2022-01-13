#show hadoop
hadoop fs -ls hdfs://master:9000/<folder_path>
#delete hadoop
hadoop fs -rm -r /user/test/sample.txt
#spark submit
spark-submit myscript.py <script_arg1> <script_arg2> … <script_argN>
