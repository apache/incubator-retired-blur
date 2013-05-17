#!/bin/sh

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

kill `cat $bin/../agent.pid`
