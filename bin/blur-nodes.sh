#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   BLUR_NODES    File naming remote hosts.
#     Default is ${BLUR_CONF_DIR}/nodes.
#   BLUR_CONF_DIR  Alternate conf dir. Default is ${BLUR_HOME}/conf.
#   BLUR_NODE_SLEEP Seconds to sleep between spawning remote commands.
#   BLUR_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: blur-nodes.sh [--config confdir] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in 
# blur-env.sh. Save it here.
HOSTLIST=$BLUR_NODES

if [ -f "${BLUR_CONF_DIR}/blur-env.sh" ]; then
  . "${BLUR_CONF_DIR}/blur-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$BLUR_NODES" = "" ]; then
    export HOSTLIST="${BLUR_CONF_DIR}/nodes"
  else
    export HOSTLIST="${BLUR_NODES}"
  fi
fi

for node in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $BLUR_SSH_OPTS $node $"${@// /\\ }" \
   2>&1 | sed "s/^/$node: /" &
 if [ "$BLUR_NODE_SLEEP" != "" ]; then
   sleep $BLUR_NODE_SLEEP
 fi
done

wait
