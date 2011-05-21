#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

$BLUR_HOME/bin/start-shards.sh
$BLUR_HOME/bin/start-controllers.sh