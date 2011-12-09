#!/usr/bin/env bash

# Copyright (C) 2011 Near Infinity Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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
while [  $INSTANCE -lt $BLUR_NUMBER_OF_CONTROLLER_SERVER_INSTANCES_PER_MACHINE ]; do
  PID_FILE=$BLUR_HOME/pids/controller-$INSTANCE.pid

  if [ -f $PID_FILE ]; then
    if kill -0 `cat $PID_FILE` > /dev/null 2>&1; then
      echo Controller server already running as process `cat $PID_FILE`.  Stop it first.
      let INSTANCE=INSTANCE+1
      continue
    fi
  fi

  LOG_NAME=blur-controller-server-$HOSTNAME-$INSTANCE
  nohup "$JAVA_HOME"/bin/java $BLUR_CONTROLLER_JVM_OPTIONS -Dblur.logs.dir=$BLUR_LOGS -Dblur.log.file=$LOG_NAME.log -cp $BLUR_CLASSPATH com.nearinfinity.blur.thrift.ThriftBlurControllerServer -s $INSTANCE > "$BLUR_LOGS/$LOG_NAME.out" 2>&1 < /dev/null &
  echo $! > $PID_FILE
  echo Controller [$INSTANCE] starting as process `cat $PID_FILE`.

  let INSTANCE=INSTANCE+1 
done