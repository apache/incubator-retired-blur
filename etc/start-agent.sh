#!/bin/sh

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

nohup $bin/bin/agent.sh $1 &
