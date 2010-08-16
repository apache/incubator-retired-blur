#!/bin/sh
. $BLUR_HOME/bin/blur_config.sh
pid=$BLUR_PID_DIR/blur_shard.pid

if [ -f $pid ]; then
  if kill -15 `cat $pid` > /dev/null 2>&1; then
    echo stopping shard `cat $pid`
  else
    echo no shard process to stop
  fi
else
  echo no shard process to stop because no pid file $pid
fi

