. $HADOOP_HOME/conf/hadoop-env.sh

export HADOOP_CLASSPATH
for f in $HADOOP_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done