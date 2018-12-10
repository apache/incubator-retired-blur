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

INSTANCE=0
while [  $INSTANCE -lt $BLUR_NUMBER_OF_SHARD_SERVER_INSTANCES_PER_MACHINE ]; do
  PID_FILE=$BLUR_HOME/pids/shard-$INSTANCE.pid

  if [ -f $PID_FILE ]; then
    if kill -0 `cat $PID_FILE` > /dev/null 2>&1; then
      echo Shard server already running as process `cat $PID_FILE`.  Stop it first.
      let INSTANCE=INSTANCE+1
      continue
    fi
  fi

  PROC_NAME=shard-server-$HOSTNAME-$INSTANCE
  nohup "$JAVA_HOME"/bin/java -Dblur.name=$PROC_NAME -Dblur-shard-$INSTANCE -Djava.library.path=$JAVA_LIBRARY_PATH  $BLUR_SHARD_JVM_OPTIONS -Dblur.logs.dir=$BLUR_LOGS -Dblur.log.file=blur-$USER-$PROC_NAME -cp $BLUR_CLASSPATH org.apache.blur.thrift.ThriftBlurShardServer -s $INSTANCE > "$BLUR_LOGS/blur-$USER-$PROC_NAME.out" 2>&1 < /dev/null &
  echo $! > $PID_FILE
  echo Shard [$INSTANCE] starting as process `cat $PID_FILE`.

  let INSTANCE=INSTANCE+1 
done


