. ~/.bash_profile
. $BLUR_HOME/conf/blur-env.sh

BLUR_CLASSPATH=$BLUR_HOME/conf

for f in $HADOOP_HOME/*.jar; do
  BLUR_CLASSPATH=${BLUR_CLASSPATH}:$f;
done

for f in $HADOOP_HOME/lib/*.jar; do
  BLUR_CLASSPATH=${BLUR_CLASSPATH}:$f;
done

for f in $BLUR_HOME/lib/*.jar; do
  BLUR_CLASSPATH=${BLUR_CLASSPATH}:$f;
done

export BLUR_CLASSPATH

HOSTNAME=`hostname`
