#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

PID_FILE=$BLUR_HOME/pids/shard.pid

if [ -f $PID_FILE ]; then
  if kill -0 `cat $PID_FILE` > /dev/null 2>&1; then
    echo Stopping Shard server with pid [`cat $PID_FILE`].
    kill `cat $PID_FILE`
  else
    echo No Shard server to stop
  fi
else
  echo No Shard server to stop
fi