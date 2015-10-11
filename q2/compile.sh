#!/usr/bin/env bash

# Export environment variable
export HADOOP_HOME=/group/home/cone/hadoop

# Compile the task
javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.2.0.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.2.0.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar -d calcmin_classes calcmin.java
jar -cvf calcmin.jar -C calcmin_classes/ .

# Submit the job
out=out-`date +%Y%m%d%H%M%S`
hadoop jar calcmin.jar org.myorg.calcmin /data ./$out
hadoop fs -cat ./$out/part-*

# Clean up all the result
hadoop fs -rm -r ./out*