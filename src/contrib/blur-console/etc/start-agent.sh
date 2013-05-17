#!/bin/sh

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

nohup $bin/agent.sh $1 &
