# Set environment specific Blur settings here.

# JAVA_HOME is required
export JAVA_HOME=/home/blur/jdk1.6.0_30

# HADOOP_HOME is required
export HADOOP_HOME=/home/blur/hadoop-0.20.2-cdh3u3

# JAVA JVM OPTIONS for the shard servers, jvm tuning parameters are placed here.
#
# This is an example of JVM options on a large heap and how to setup large pages and max direct memory size.
# export BLUR_SHARD_JVM_OPTIONS="-XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages -Xms12g -Xmx12g -Xmn2g -XX:+UseCompressedOops \
#-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:CMSIncrementalDutyCycleMin=10 -XX:CMSIncrementalDutyCycle=50 \
#-XX:ParallelGCThreads=8 -XX:+UseParNewGC -XX:MaxGCPauseMillis=200 -XX:GCTimeRatio=10 -XX:+DisableExplicitGC \
#-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$BLUR_HOME/logs/gc-blur-shard-server_`date +%Y%m%d_%H%M%S`.log
export BLUR_SHARD_JVM_OPTIONS="-Xmx256m -XX:+UseCompressedOops"

# JAVA JVM OPTIONS for the shard servers, jvm tuning parameters are placed here.
export BLUR_CONTROLLER_JVM_OPTIONS="-Xmx128m -XX:+UseCompressedOops"

# JAVA JVM OPTIONS for the shard servers, jvm tuning parameters are placed here.
export BLUR_COMMAND="-Xmx128m -XX:+UseCompressedOops"

# Any SSH Options to be used during startup or shutdown commands.
export BLUR_SSH_OPTS=

# Time to sleep between shard server commands.
export BLUR_SHARD_SLEEP=0.1

# Time to sleep between controller server commands.
export BLUR_CONTROLLER_SLEEP=0.1

# The of shard servers to spawn per machine.
export BLUR_NUMBER_OF_SHARD_SERVER_INSTANCES_PER_MACHINE=3

# The of controller servers to spawn per machine.
export BLUR_NUMBER_OF_CONTROLLER_SERVER_INSTANCES_PER_MACHINE=2

# The directory where all the log files will be located.
# export BLUR_LOGS=$BLUR_HOME/logs

