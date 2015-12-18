#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

for f in $bin/../lib/*.*jar; do
   CLASSPATH=${CLASSPATH}:$f;
done;
exec java -classpath "$CLASSPATH" IndexService.IndexServer $@ ${HADOOP_HOME}/conf/core-site.xml ${HADOOP_HOME}/conf/hdfs-site.xml ${HADOOP_HOME}/conf/mapred-site.xml ../conf/hive-default.xml  
