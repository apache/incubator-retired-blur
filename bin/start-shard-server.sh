. $BLUR_HOME/bin/blur-config.sh

nohup "$JAVA_HOME"/bin/java $BLUR_SHARD_JVM_OPTIONS -cp $BLUR_CLASSPATH com.nearinfinity.blur.thrift.ThriftBlurShardServer > "$BLUR_HOME/logs/blur-shard-server.out" 2>&1 < /dev/null &