#!/bin/sh

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

cat $bin/agent.pid | xargs -|1 kill
