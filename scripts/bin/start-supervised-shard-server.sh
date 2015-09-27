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

echo "Sourcing configs.."
. $BLUR_HOME/bin/blur-supervised-config.sh

PROC_NAME=shard-server-$HOSTNAME-0
echo "Launching shard [$PROC_NAME] now..."
echo "Using BLUR_CLASSPATH: ${BLUR_CLASSPATH}"
exec "$JAVA_HOME"/bin/java -Dblur.name=$PROC_NAME -Dblur-shard -Dlog4j.configuration=log4j.properties $BLUR_SHARD_JVM_OPTIONS -Djava.library.path=$JAVA_LIBRARY_PATH -cp "$BLUR_CLASSPATH" org.apache.blur.thrift.ThriftBlurShardServer -s 0 
