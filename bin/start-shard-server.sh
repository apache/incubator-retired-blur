#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

PID_FILE=$BLUR_HOME/pids/shard.pid

if [ -f $PID_FILE ]; then
  if kill -0 `cat $PID_FILE` > /dev/null 2>&1; then
    echo Shard server already running as process `cat $PID_FILE`.  Stop it first.
    exit 1
  fi
fi

LOG_NAME=blur-shard-server-$HOSTNAME
nohup "$JAVA_HOME"/bin/java $BLUR_SHARD_JVM_OPTIONS -Dblur.logs.dir=$BLUR_HOME/logs -Dblur.log.file=$LOG_NAME.log -cp $BLUR_CLASSPATH com.nearinfinity.blur.thrift.ThriftBlurShardServer > "$BLUR_HOME/logs/$LOG_NAME.out" 2>&1 < /dev/null &
echo $! > $PID_FILE
echo Shard starting as process `cat $PID_FILE`.
