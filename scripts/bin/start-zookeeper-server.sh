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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/blur-config.sh

if [ $BLUR_MANAGE_ZK = true ]; then
  PID_FILE=$BLUR_HOME/pids/zk.pid

  if [ -f $PID_FILE ]; then
    if kill -0 `cat $PID_FILE` > /dev/null 2>&1; then
      echo ZooKeeper already running as process `cat $PID_FILE`.  Stop it first.
      exit 0
    fi
  fi

  PROC_NAME=zk-$HOSTNAME
  nohup "$JAVA_HOME"/bin/java -Dblur.name=$PROC_NAME -Djava.library.path=$JAVA_LIBRARY_PATH -Dblur-zk $BLUR_ZK_JVM_OPTIONS -Dblur.logs.dir=$BLUR_LOGS -Dblur.log.file=blur-$USER-$PROC_NAME -Dlog4j.configuration=file://$BLUR_HOME/conf/log4j-command.xml -cp $BLUR_CLASSPATH org.apache.zookeeper.server.quorum.QuorumPeerMain $BLUR_HOME/conf/default_zoo.cfg > "$BLUR_LOGS/blur-$USER-$PROC_NAME.out" 2>&1 < /dev/null &
  echo $! > $PID_FILE
  echo ZooKeeper starting as process `cat $PID_FILE`.
else 
  echo Blur is not managing ZooKeeper
fi