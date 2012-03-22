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

export BLUR_HOME="$bin"/..
export BLUR_HOME_CONF=$BLUR_HOME/conf

. $BLUR_HOME/conf/blur-env.sh
if [ -z "$JAVA_HOME" ]; then
  cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|       > http://java.sun.com/javase/downloads/ <                      |
|                                                                      |
| Hadoop and Blur requires Java 1.6 or later.                          |
| NOTE: This script will find Sun Java whether you install using the   |
|       binary or the RPM based installer.                             |
+======================================================================+
EOF
  exit 1
fi

if [ -z "$HADOOP_HOME" ]; then
  cat 1>&2 <<EOF
+======================================================================+
|      Error: HADOOP_HOME is not set                                   |
+----------------------------------------------------------------------+
| Please download the stable Hadoop version from Apache web site       |
|       > http://hadoop.apache.org/ <                                  |
|                                                                      |
| Blur requires Hadoop 0.20.205 or later.                              |
+======================================================================+
EOF
  exit 1
fi

export JAVA=$JAVA_HOME/bin/java

export BLUR_LOGS=${BLUR_LOGS:=$BLUR_HOME/logs}

if [ ! -d "$BLUR_LOGS" ]; then
  mkdir -p $BLUR_LOGS
fi

if [ ! -d "$BLUR_HOME/pids" ]; then
  mkdir -p $BLUR_HOME/pids
fi

BLUR_CLASSPATH=$BLUR_HOME/conf

for f in $HADOOP_HOME/*.jar; do
  BLUR_CLASSPATH=${BLUR_CLASSPATH}:$f;
done

for f in $HADOOP_HOME/lib/*.jar; do
  BLUR_CLASSPATH=${BLUR_CLASSPATH}:$f;
done

for f in $BLUR_HOME/lib/*.jar; do
  BLUR_CLASSPATH=${BLUR_CLASSPATH}:$f;
done

export BLUR_CLASSPATH

# setup 'java.library.path' for native-hadoop code if necessary
if [ -d "${HADOOP_HOME}/build/native" -o -d "${HADOOP_HOME}/lib/native" -o -d "${HADOOP_HOME}/sbin" ]; then
  JAVA_PLATFORM=`CLASSPATH=${BLUR_CLASSPATH} ${JAVA} -Xmx32m ${HADOOP_JAVA_PLATFORM_OPTS} org.apache.hadoop.util.PlatformName | sed -e "s/ /_/g"`

  if [ -d "${HADOOP_HOME}/lib/native" ]; then
    if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
      JAVA_LIBRARY_PATH=${JAVA_LIBRARY_PATH}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
    else
      JAVA_LIBRARY_PATH=${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
    fi
  fi
fi

HOSTNAME=`hostname`
