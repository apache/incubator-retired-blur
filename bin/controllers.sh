#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

export HOSTLIST="${BLUR_HOME_CONF}/controllers"

for controller in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $BLUR_SSH_OPTS $controller $"${@// /\\ }" \
   2>&1 | sed "s/^/$controller: /" &
 if [ "$BLUR_CONTROLLER_SLEEP" != "" ]; then
   sleep $BLUR_CONTROLLER_SLEEP
 fi
done

wait

