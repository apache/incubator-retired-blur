. $BLUR_HOME/conf/blur-env.sh

$HADOOP_HOME/bin/hadoop-daemons.sh --config $BLUR_HOME/conf --hosts shards stop com.nearinfinity.blur.thrift.ThriftBlurShardServer
