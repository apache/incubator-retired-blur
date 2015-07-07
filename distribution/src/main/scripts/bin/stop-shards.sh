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

if [[ -z $BLUR_CLASSPATH ]] ; then
	. "$bin"/blur-config.sh
fi
PROC_NAME="Shutdown"
nohup "$JAVA_HOME"/bin/java -Dblur.name=SHUTDOWN_PROC -Djava.library.path=$JAVA_LIBRARY_PATH -Dblur.logs.dir=$BLUR_LOGS -Dblur.log.file=blur-$USER-$PROC_NAME -cp $BLUR_CLASSPATH org.apache.blur.thrift.Shutdown > "$BLUR_LOGS/blur-$USER-$PROC_NAME.out" 2>&1 < /dev/null &
SHUTDOWN_PID=$!
$BLUR_HOME/bin/shards.sh $BLUR_HOME/bin/stop-shard-server.sh
kill -15 $SHUTDOWN_PID

