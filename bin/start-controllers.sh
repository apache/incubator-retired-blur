. $BLUR_HOME/conf/blur-env.sh

echo $HADOOP_HOME

hadoop-daemons.sh --config $BLUR_HOME/conf --hosts shards start com.nearinfinity.blur.thrift.ThriftBlurShardServer

