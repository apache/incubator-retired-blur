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

# Set environment specific Blur settings here.

# JAVA_HOME is required
# export JAVA_HOME=/usr/lib/j2sdk1.6-sun

# HADOOP_HOME is required
# export HADOOP_HOME=/var/hadoop-0.20.2

# JAVA JVM OPTIONS for the shard servers, jvm tuning parameters are placed here.
#
# This is an example of JVM options on a large heap and how to setup large pages and max direct memory size.
# export BLUR_SHARD_JVM_OPTIONS="-XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages -Xms12g -Xmx12g -Xmn2g -XX:+UseCompressedOops \
#-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:CMSIncrementalDutyCycleMin=10 -XX:CMSIncrementalDutyCycle=50 \
#-XX:ParallelGCThreads=8 -XX:+UseParNewGC -XX:MaxGCPauseMillis=200 -XX:GCTimeRatio=10 -XX:+DisableExplicitGC \
#-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$BLUR_HOME/logs/gc-blur-shard-server_`date +%Y%m%d_%H%M%S`.log
# Consider adding the -XX:OnOutOfMemoryError="kill -9 %p" option to kill jvms that are failing due to memory issues.
export BLUR_SHARD_JVM_OPTIONS="-Xmx1024m -Djava.net.preferIPv4Stack=true -XX:MaxDirectMemorySize=256m "

# JAVA JVM OPTIONS for the controller servers, jvm tuning parameters are placed here.
# Consider adding the -XX:OnOutOfMemoryError="kill -9 %p" option to kill jvms that are failing due to memory issues.
export BLUR_CONTROLLER_JVM_OPTIONS="-Xmx1024m -Djava.net.preferIPv4Stack=true "

# This tells blur to manage the ZooKeeper instances, set this to false if Blur is not going to manage the ZooKeeper cluster
export BLUR_MANAGE_ZK=true

# JAVA JVM OPTIONS for the zookeepers servers, jvm tuning parameters are placed here.
export BLUR_ZK_JVM_OPTIONS="-Xmx128m -Djava.net.preferIPv4Stack=true "

# JAVA JVM OPTIONS for the console servers, jvm tuning parameters are placed here.
export BLUR_CONSOLE_JVM_OPTIONS="-Xmx1024m -Djava.net.preferIPv4Stack=true "

# JAVA JVM OPTIONS for the shard servers, jvm tuning parameters are placed here.
export BLUR_COMMAND="-Xmx1024m -Djava.net.preferIPv4Stack=true"

# Any SSH Options to be used during startup or shutdown commands.
export BLUR_SSH_OPTS=

# Time to sleep between shard server commands.
export BLUR_SHARD_SLEEP=0.1

# Time to sleep between controller server commands.
export BLUR_CONTROLLER_SLEEP=0.1

# Time to sleep between zookeeper server commands.
export BLUR_ZK_SLEEP=0.1

# The of shard servers to spawn per machine.
export BLUR_NUMBER_OF_SHARD_SERVER_INSTANCES_PER_MACHINE=1

# The of controller servers to spawn per machine.
export BLUR_NUMBER_OF_CONTROLLER_SERVER_INSTANCES_PER_MACHINE=1

# The directory where all the log files will be located.
# export BLUR_LOGS=$BLUR_HOME/logs

# If you would like to add Hadoop native libraries or any other native libs add them to this variable.
#export JAVA_LIBRARY_PATH=

