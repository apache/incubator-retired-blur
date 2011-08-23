#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

$BLUR_HOME/bin/start-controller-server.sh




