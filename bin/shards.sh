#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

export HOSTLIST="${BLUR_HOME_CONF}/shards"

for shard in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $BLUR_SSH_OPTS $shard $"${@// /\\ }" \
   2>&1 | sed "s/^/$shard: /" &
 if [ "$BLUR_SHARD_SLEEP" != "" ]; then
   sleep $BLUR_SHARD_SLEEP
 fi
done

wait

